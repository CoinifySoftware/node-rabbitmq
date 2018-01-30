'use strict';

const _ = require('lodash'),
  amqplib = require('amqplib'),
  backoff = require('backoff'),
  consoleLogLevel = require('console-log-level'),
  uuid = require('uuid'),
  util = require('util'),
  EventEmitter = require('events');

const localConfig = require('../config.local');

const DEFAULT_CONFIG = {
  connection: {
    host: 'localhost',
    port: 5672,
    protocol: 'amqp',
    vhost: '',
    username: 'guest',
    password: 'guest'
  },
  channel: {
    prefetch: 16
  },
  service: {
    name: 'defaultService'
  },
  exchanges: {
    retry: '_retry',
    tasksTopic: 'tasks.topic',
    failed: '_failed',
    eventsTopic: 'events.topic'
  },
  queues: {
    retryPrefix: '_retry',
    failed: '_failed'
  },
  consumer: {
    prefetch: 2
  },
  defaultLogLevel: 'error'
};

class CoinifyRabbit {
  /**
   * Construct CoinifyRabbit object.
   *
   * @param {object} options Override default configuration values.
   *                         See {@file config/default.js} for supported configuration values.
   * @param {object} options.logger Logging class with Bunyan-compatible interface
   * @param {object} options.service Service-specific options
   * @param {string} options.service.name Service name, used to group consumers from the same service together.
   *
   */
  constructor(options = {}) {
    options = _.clone(options);
    this._config = _.defaultsDeep({}, options, localConfig, DEFAULT_CONFIG);

    this._logger = options.logger || consoleLogLevel({level: _.get(this._config, 'defaultLogLevel')});
    _.pull(options, ['logger']);

    if (!_.has(this._config, 'service.name')) {
      throw new Error('options.service.name must be set');
    }

    this._isShuttingDown = false;

    /**
     * Array of registered consumers in the current channel.
     *
     * 'key' property for 'event' type is eventKey, for 'task' type it is taskName
     *
     * Note: This array is cleared by the _onChannelClosed() function
     *
     * @type {{type: 'event'|'task', consumerTag: string, key: string, consumeFn: function, options: object}[]}
     * @private
     */
    this._registeredConsumers = [];

    /**
     * Messages that are currently being consumed
     *
     * @type {{context: object, uuid: string, time: int, eventName: string|undefined, taskName: string|undefined}}
     * @private
     */
    this._activeMessageConsumptions = [];
  }

  /**
   * Emit an event to the global event topic exchange
   *
   * The full event name is used as the routing key
   *
   * @param {string} eventName Name of event to emit. Will be prefixed with '<service.name>.'
   * @param {object} context Object of context data to emit along with the event
   * @param {object} options Object of optional arguments
   * @param {string} options.uuid Pre-defined UUID to use for the task
   * @param {number} options.time Pre-defined timestamp to use for the task
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange()
   * @param {boolean} options.exchange.durable If true, the exchange will survive broker restarts. Defaults to true
   * @param {boolean} options.exchange.autoDelete If true, the exchange will be destroyed once the number of bindings
   *                                              for which it is the source drop to zero. Defaults to false.
   * @param {string} options.service.name Custom service.name to use for the RabbitMQ queue name, useful for testing.
   * @return {Promise<{eventName, context, uuid, time}>}
   */
  async emitEvent(eventName, context, options = {}) {
    const serviceName = _.get(_.defaultsDeep({}, options, this._config), 'service.name');
    // Prefix with service name and a dot to get full event name
    const fullEventName = serviceName ?
      serviceName + '.' + eventName :
      eventName;
    this._logger.trace({fullEventName, context, options}, 'emitEvent()');
    const channel = await this._getChannel();

    const exchangeName = _.get(this._config, 'exchanges.eventsTopic');
    await channel.assertExchange(exchangeName, 'topic', _.get(options, 'exchange', {}));

    const event = {
      eventName: fullEventName,
      context,
      uuid: options.uuid || uuid.v4(),
      time: options.time ? new Date(options.time).getTime() : Date.now(),
      attempts: 0
    };

    const message = new Buffer(JSON.stringify(event));

    const publishResult = await channel.publish(exchangeName, fullEventName, message);
    if (!publishResult) {
      throw new Error('channel.publish() resolved to ' + JSON.stringify(publishResult));
    }

    this._logger.info({event, exchangeName, options}, 'Event emitted');

    return event;
  }

  /**
   * Register a consumer for an event.
   *
   * The configuration variable service.name decides the name of the queue to consume from:
   *   Consumers with the same service.name will consume from the same queue (each event will be consumed once).
   *   Consumers with different service.name will consume from different queues (each event will be consumed once per service.name)
   *
   * @param {string} eventKey Name of event to consume, allowing for wildcards (See rules for RabbitMQ topic exchange)
   * @param {function<Promise>} consumeFn Function that will be called for each event to consume.
   *                                      The following arguments are passed to the function:
   *                                      - {object} context Context for the event
   *                                      - {object} event Other event data, such as eventName, uuid, time
   * @param {object} options Object of optional arguments
   * @param {string} options.consumerTag Explicit consumer tag to use
   * @param {function<Promise>} options.onCancel Function which will be called if the consumer was cancelled.
   *                                             It has not yet been possible to test this functionality.
   * @param {function<Promise>} options.onError Function which will be called with (err, context, event) if consumeFn rejected.
   *                                            If this function rejects or is not given, an unhandled rejection will appear.
   * @param {object} options.queue Object of options to pass to amqplib's assertQueue()
   * @param {boolean} options.queue.durable If true, the queue will survive broker restarts,
   *                                        modulo the effects of exclusive and autoDelete;
   *                                        This defaults to true if not supplied, unlike the others
   * @param {boolean} options.queue.autoDelete If true, the queue will be destroyed once the number of consumers drops
   *                                           to zero. Defaults to false.
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange()
   * @param {boolean} options.exchange.durable If true, the exchange will survive broker restarts. Defaults to true
   * @param {boolean} options.exchange.autoDelete If true, the exchange will be destroyed once the number of bindings
   *                                              for which it is the source drop to zero. Defaults to false.
   * @param {number} options.consumer.prefetch Sets the limit for number of unacknowledged messages for this consumer.
   *                                           Defaults to the consumer.prefetch configuration value.
   *                                           If this option is specified, all calls to this function (and registerEventConsumer)
   *                                           must be done serially to avoid race conditions, causing incorrect prefetch values to be set
   * @param {object|false} options.retry Specify how (if at all) performing the task should be retried on failure (if `consumeFn` rejects).
   * @param {object} options.retry.backoff Backoff configuration for retrying tasks
   * @param {string} options.retry.backoff.type Type of backoff: 'exponential' or 'fixed'. Defaults to 'fixed'
   *                                            For exponential backoff, the delay until next retry is calculated as (delay * (base ^ n)),
   *                                            where n is the current attempt (0-indexed). First retry is thus always after `delay` seconds
   *                                            For fixed backoff, the delay until next retry is always options.backoff.delay
   * @param {number} options.retry.backoff.delay Delay in seconds. Defaults to 16 seconds
   * @param {number} options.retry.backoff.base (Only for exponential backoff) The base number for the exponentiation. Defaults to 2
   * @param {number} options.retry.maxAttempts The maximum number of retry attempts. Defaults to 12.
   *                                           If set to 1, the task will at most be run twice:
   *                                             One for the original attempt, and one retry attempt.
   *                                           Setting this to 0 is the same as setting options.retry: false
   * @param {string} options.service.name Custom service.name to use for the RabbitMQ queue name, useful for testing.
   * @return {Promise<string>} Returns the consumer tag which is needed to cancel the consumer
   */
  async registerEventConsumer(eventKey, consumeFn, options = {}) {
    await CoinifyRabbit.validateConsumerRetryOptions(options.retry);

    const serviceName = _.get(_.defaultsDeep({}, options, this._config), 'service.name');
    const eventQueueName = CoinifyRabbit._getConsumeEventQueueName(eventKey, serviceName);
    const exchangeName = _.get(this._config, 'exchanges.eventsTopic');

    const consumeMessageOptions = _.defaultsDeep({queueName: eventQueueName}, options);

    this._logger.trace({eventKey, eventQueueName}, 'registerEventConsumer()');

    const channel = await this._getChannel();

    await channel.assertExchange(exchangeName, 'topic', _.get(options, 'exchange', {}));

    const q = await channel.assertQueue(eventQueueName, _.get(options, 'queue', {}));
    await channel.bindQueue(q.queue, exchangeName, eventKey);

    const prefetch = _.get(_.defaultsDeep({}, options, this._config), 'consumer.prefetch');
    await channel.prefetch(prefetch, false);
    const {consumerTag} = await channel.consume(q.queue,
      async (message) => this._handleConsumeMessage(message, 'event', consumeMessageOptions, consumeFn),
      {consumerTag: options.consumerTag}
    );

    this._registeredConsumers.push({type: 'event', key: eventKey, consumerTag, consumeFn, options});

    return consumerTag;
  }

  /**
   * Enqueue a task using the global task topic exchange
   *
   * The full task name is used as the routing key
   *
   * @param {string} fullTaskName Full name of task to enqueue (including 'service-name.' prefix)
   * @param {object} context Object of context data to pass along with the task
   * @param {object} options Object of optional arguments
   * @param {string} options.uuid Pre-defined UUID to use for the task
   * @param {number} options.time Pre-defined timestamp to use for the task
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange()
   * @param {boolean} options.exchange.durable If true, the exchange will survive broker restarts. Defaults to true
   * @param {boolean} options.exchange.autoDelete If true, the exchange will be destroyed once the number of bindings
   *                                              for which it is the source drop to zero. Defaults to false.
   * @return {Promise<{taskName, context, uuid, time}>}
   */
  async enqueueTask(fullTaskName, context, options = {}) {
    const channel = await this._getChannel();
    const exchangeName = _.get(this._config, 'exchanges.tasksTopic');
    this._logger.trace({fullTaskName, context, exchangeName, options}, 'enqueueTask()');

    await channel.assertExchange(exchangeName, 'topic', _.get(options, 'exchange', {}));

    const serviceName = _.get(_.defaultsDeep({}, options, this._config), 'service.name');
    const task = {
      taskName: fullTaskName,
      context,
      uuid: options.uuid || uuid.v4(),
      time: options.time ? new Date(options.time).getTime() : Date.now(),
      attempts: 0,
      origin: serviceName
    };

    const message = new Buffer(JSON.stringify(task));

    const publishResult = await channel.publish(exchangeName, fullTaskName, message);
    if (!publishResult) {
      throw new Error('channel.publish() resolved to ' + JSON.stringify(publishResult));
    }

    this._logger.info({task, exchangeName, options}, 'Enqueued task');

    return task;
  }

  /**
   * Consume a task.
   *
   * The configuration variable service.name decides the name of the queue to consume from:
   *   For e.g. a taskName of 'my-task' and a service.name of 'my-service', the task queue will be 'my-service.my-task'
   *
   * @param {string} taskName Name of task to consume. Prefixed with service name and dot (.)
   * @param {function<Promise>} consumeFn Function that will be called for each task to consume.
   *                                      The following arguments are passed to the function:
   *                                      - {object} context Context for the task
   *                                      - {object} task Other task data, such as taskName, uuid, time
   * @param {object} options Object of optional arguments
   * @param {string} options.consumerTag Explicit consumer tag to use
   * @param {function<Promise>} options.onCancel Function which will be called if the consumer was cancelled.
   *                                             It has not yet been possible to test this functionality.
   * @param {function<Promise>} options.onError Function which will be called if consumeFn rejected.
   *                                            Argument to function is a single object with the following properties:
   *                                              - {Error} err The error that consumeFn rejected with
   *                                              - {object} context The task context
   *                                              - {object} task Extra task data, such as taskName, uuid, time
   *                                              - {boolean} willRetry Whether or not retrying consumption will be attempted later
   *                                              - {number} delaySeconds Number of seconds to delay before retrying
   *                                            If this function is not given, the error will be logged with 'warn' level.
   *                                            If this function is given and rejects, the rejection will be logged with 'error' level.
   *                                            If this function resolves, nothing will be logged.
   * @param {object} options.queue Object of options to pass to amqplib's assertQueue() for both task and retry queues
   * @param {boolean} options.queue.durable If true, the queue will survive broker restarts,
   *                                        modulo the effects of exclusive and autoDelete;
   *                                        This defaults to true if not supplied, unlike the others
   * @param {boolean} options.queue.autoDelete If true, the queue will be destroyed once the number of consumers drops
   *                                           to zero. Defaults to false.
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange() for both task and retry exchanges
   * @param {boolean} options.exchange.durable If true, the exchange will survive broker restarts. Defaults to true
   * @param {boolean} options.exchange.autoDelete If true, the exchange will be destroyed once the number of bindings
   *                                              for which it is the source drop to zero. Defaults to false.
   * @param {number} options.consumer.prefetch Sets the limit for number of unacknowledged messages for this consumer.
   *                                           Defaults to the consumer.prefetch configuration value.
   *                                           If this option is specified, all calls to this function (and registerEventConsumer)
   *                                           must be done serially to avoid race conditions, causing incorrect prefetch values to be set
   * @param {object|false} options.retry Specify how (if at all) performing the task should be retried on failure (if `consumeFn` rejects).
   * @param {object} options.retry.backoff Backoff configuration for retrying tasks
   * @param {string} options.retry.backoff.type Type of backoff: 'exponential' or 'fixed'. Defaults to 'fixed'
   *                                            For exponential backoff, the delay until next retry is calculated as (delay * (base ^ n)),
   *                                            where n is the current attempt (0-indexed). First retry is thus always after `delay` seconds
   *                                            For fixed backoff, the delay until next retry is always options.backoff.delay
   * @param {number} options.retry.backoff.delay Delay in seconds. Defaults to 16 seconds
   * @param {number} options.retry.backoff.base (Only for exponential backoff) The base number for the exponentiation. Defaults to 2
   * @param {number} options.retry.maxAttempts The maximum number of retry attempts. Defaults to 12.
   *                                           If set to 1, the task will at most be run twice:
   *                                             One for the original attempt, and one retry attempt.
   *                                           Setting this to 0 is the same as setting options.retry: false
   * @param {string} options.service.name Custom service.name to use for the queue and full task name, useful for testing.
   * @return {Promise<string>} Returns the consumer tag which is needed to cancel the consumer
   */
  async registerTaskConsumer(taskName, consumeFn, options = {}) {
    await CoinifyRabbit.validateConsumerRetryOptions(options.retry);

    const serviceName = _.get(_.defaultsDeep({}, options, this._config), 'service.name');
    const fullTaskName = serviceName + '.' + taskName;
    const taskQueueName = CoinifyRabbit._getregisterTaskConsumerQueueName(taskName, serviceName);
    const exchangeName = _.get(this._config, 'exchanges.tasksTopic');

    const consumeMessageOptions = _.defaultsDeep({queueName: taskQueueName}, options);

    this._logger.trace({taskName, fullTaskName, taskQueueName, exchangeName, options}, 'registerTaskConsumer()');

    const channel = await this._getChannel();

    await channel.assertExchange(exchangeName, 'topic', _.get(options, 'exchange', {}));

    const q = await channel.assertQueue(taskQueueName, _.get(options, 'queue', {}));
    await channel.bindQueue(q.queue, exchangeName, fullTaskName);

    const prefetch = _.get(_.defaultsDeep({}, options, this._config), 'consumer.prefetch');
    await channel.prefetch(prefetch, false);
    const {consumerTag} = await channel.consume(q.queue,
      async (message) => this._handleConsumeMessage(message, 'task', consumeMessageOptions, consumeFn),
      {consumerTag: options.consumerTag}
    );

    this._registeredConsumers.push({type: 'task', key: taskName, consumerTag, consumeFn, options});

    return consumerTag;
  }

  /**
   * Register a consumer for failed messages
   * This consumer will consume messages from the failed queue, defined either in the configuration or default configuration
   *
   * @param {function<Promise>} consumeFn Function that will be called for each message to consume.
   *                                      The following arguments are passed to the function:
   *                                      - {object} context Context for the message
   *                                      - {object} message Full message object, currently either a task or event
   * @param {object} options Object of optional arguments
   * @param {object} options.queue Object of options to pass to amqplib's assertQueue()
   * @param {boolean} options.queue.durable If true, the queue will survive broker restarts,
   *                                        modulo the effects of exclusive and autoDelete;
   *                                        This defaults to true if not supplied, unlike the others
   * @param {boolean} options.queue.autoDelete If true, the queue will be destroyed once the number of consumers drops
   *                                           to zero. Defaults to false.
   * @param {number} options.consumer.prefetch Sets the limit for number of unacknowledged messages for this consumer.
   *                                           Defaults to the consumer.prefetch configuration value.
   *                                           If this option is specified, all calls to this function (and registerEventConsumer)
   *                                           must be done serially to avoid race conditions, causing incorrect prefetch values to be set
   * @return {Promise<string>} Returns the consumer tag which is needed to cancel the consumer
   */
  async registerFailedMessageConsumer(consumeFn, options = {}) {
    const channel = await this._getChannel();
    const queueName = _.get(this._config, 'queues.failed');
    this._logger.trace({queueName}, 'registerFailedMessageConsumer()');

    const q = await channel.assertQueue(queueName, _.get(options, 'queue', {}));
    const prefetch = _.get(_.defaultsDeep({}, options, this._config), 'consumer.prefetch');
    await channel.prefetch(prefetch, false);
    const {consumerTag} = await channel.consume(q.queue,
      async (message) => this._handleFailedMessage(message, options, consumeFn),
      {consumerTag: options.consumerTag}
    );

    this._registeredConsumers.push({type: 'message', key: 'failed', consumerTag, consumeFn, options});

    return consumerTag;
  }

  /**
   * Get a channel in RabbitMQ
   *
   * @return {Promise<amqplib.Channel>}
   * @private
   */
  async _getChannel() {
    if (!this._channel) {
      /*
       * In order to not create multiple channels when invoked multiple times, while still waiting for the
       * first channel to be created, we will create a promise that subsequent invocations will detect and wait for.
       */
      if (!this._getChannelPromise) {
        // Create and start executing promise immediately
        this._getChannelPromise = (async () => {
          try {
            const connection = await this._getConnection();
            this._logger.info({}, 'Opening channel');

            this._channel = await connection.createChannel();

            this._channel.on('error', err => {
              // Basic error handling: Log error and discard current channel, so a new one will be created next time
              this._logger.warn({err}, 'RabbitMQ channel error');
              delete this._channel;
            });
            this._channel.on('close', () => {
              delete this._channel;
              this._onChannelClosed();
            });

            await this._onChannelOpened(this._channel);
          }
          finally {
            delete this._getChannelPromise;
          }
        })();
      }

      await this._getChannelPromise;

    }
    return this._channel;
  }

  /**
   * Get a singleton connection to RabbitMQ
   * @return {Promise<amqplib.Connection>}
   * @private
   */
  async _getConnection() {
    if (!this._conn) {
      if (this._isShuttingDown) {
        throw new Error('RabbitMQ module is shutting down');
      }

      /*
       * In order to not create multiple connections when invoked multiple times, while still waiting for the
       * first connection to be created, we will create a promise that subsequent invocations will detect and wait for.
       */
      if (!this._getConnectionPromise) {
        // Create and start executing promise immediately
        this._getConnectionPromise = (async () => {
          try {
            const connectionConfig = _.get(this._config, 'connection');
            this._logger.info({connectionConfig: _.omit(connectionConfig, ['password'])}, 'Opening connection to RabbitMQ');

            this._conn = await amqplib.connect(CoinifyRabbit._generateConnectionUrl(connectionConfig));
            this._conn.on('error', err => {
              // Basic error handling: Log error and discard current connection, so a new one will be created next time
              this._logger.warn({err}, 'RabbitMQ connection error');
              delete this._conn;
            });
            this._conn.on('close', () => {
              delete this._conn;
              this._logger.warn({}, 'Connection closed');
            });

            this._logger.info({}, 'Connection opened');
          } finally {
            delete this._getConnectionPromise;
          }
        })();
      }

      await this._getConnectionPromise;
    }

    return this._conn;
  }

  /**
   * Generates an AMQP connection URL from a configuration object.
   *
   * @see https://www.rabbitmq.com/uri-spec.html
   * Doesn't support the "query" part, as we haven't had a need for it yet.
   *
   * @param {object} connectionConfig connection object from configuration
   * @return {string} Connection URL
   * @private
   */
  static _generateConnectionUrl(connectionConfig) {
    // Check for valid protocol
    if (!_.includes(['amqp', 'amqps'], connectionConfig.protocol)) {
      throw new Error(`Invalid protocol '${connectionConfig.protocol}'. Must be 'amqp' or 'amqps'`);
    }

    let url = `${connectionConfig.protocol}://`;
    if (connectionConfig.username) {
      url += connectionConfig.username;
      if (connectionConfig.password) {
        url += `:${connectionConfig.password}`;
      }
      url += '@';
    }
    url += connectionConfig.host;
    if (connectionConfig.port) {
      url += `:${connectionConfig.port}`;
    }
    if (connectionConfig.vhost) {
      url += `/${connectionConfig.vhost}`;
    }
    return url;
  }

  /**
   * Function that is run when a (the only) channel has been opened.
   *
   * This will re-attach event and task consumers to their respective queues
   *
   * @param {amqplib.Channel} channel
   * @return {Promise.<void>}
   * @private
   */
  async _onChannelOpened(channel) {
    this._logger.info({}, 'Channel opened');

    const prefetch = _.get(this._config, 'channel.prefetch');
    await channel.prefetch(prefetch, true);

    await this._recreateRegisteredConsumers();
  }

  /**
   * Re-attach event and task consumers to their respective queues
   *
   * @return {Promise.<void>}
   * @private
   */
  async _recreateRegisteredConsumers() {
    const consumers = _.cloneDeep(this._registeredConsumers);
    this._registeredConsumers = [];

    /*
     * Re-create all registered consumers
     */
    for (const {type, key, consumerTag, consumeFn, options} of consumers) {
      // Require same consumerTag as originally used
      options.consumerTag = consumerTag;
      switch (type) {
        case 'event':
          await this.registerEventConsumer(key, consumeFn, options);
          break;
        case 'task':
          await this.registerTaskConsumer(key, consumeFn, options);
          break;
        default:
          throw new Error(`Internal error: Unknown consumer type '${type}'`);
      }
    }
  }

  /**
   * Function that is run when a channel closes..
   *
   * If channel was requested closed by calling #close(), no action is taken.
   * Otherwise, if there are registered consumers, we will try to reconnect with backoff.
   *
   * @return {Promise.<void>}
   * @private
   */
  async _onChannelClosed() {
    if (this._isShuttingDown) {
      this._logger.info({}, 'Channel closed');
      // Channel close requested. We won't try to reconnect
      return;
    }

    this._logger.warn({}, 'Channel closed unexpectedly.');

    if (!this._registeredConsumers.length) {
      // No registered consumers, no need to reconnect automatically.
      return;
    }

    return this._connectWithBackoff();
  }

  /**
   * Connects and tries to create a channel using fibonacci backoff
   *
   * @return {Promise.<void>}
   * @private
   */
  async _connectWithBackoff() {
    // Attempts to reconnect after 1, 1, 2, 3, 5, 10, 20, 30, 50, 60, 60, 60... seconds
    const fibonacciBackoff = backoff.fibonacci({
      initialDelay: 1000,
      maxDelay: 600000
    });

    fibonacciBackoff.on('ready', (number, delay) => {
      this._logger.info({}, `Connecting... (attempt ${number}, delay ${delay}ms)`);

      // If we're already attempting to connect, no reason to add another listener to the promise
      if (this._getChannelPromise) {
        return;
      }

      // Try to connect to a channel
      this._getChannel()
        // If error, log and retry again later
        .catch(err => {
          fibonacciBackoff.backoff();
          this._logger.warn({err}, 'Error reconnecting to channel');
        });
    });

    fibonacciBackoff.backoff();
  }

  /**
   * Perform graceful shutdown, optionally with a timeout.
   *
   * @param {int|null} timeout Number of milliseconds to wait for all consumer functions to finish. Passing anything else
   * than a positive integer will result in no timeout.
   * @return {Promise.<void>}
   */
  async shutdown(timeout = null) {
    if (this._isShuttingDown) {
      return;
    }
    this._isShuttingDown = true;

    /*
     * Log information about shutdown process
     */
    const activeTaskConsumptions = _.filter(this._activeMessageConsumptions, c => !!c.taskName);
    const activeEventConsumptions = _.filter(this._activeMessageConsumptions, c => !!c.eventName);

    this._logger.info({
      registeredConsumers: JSON.stringify(this._registeredConsumers.map(c => _.pick(c, ['type', 'key', 'consumerTag']))),
      activeConsumption: {
        tasks: JSON.stringify(activeTaskConsumptions.map(c => _.pick(c, ['uuid', 'taskName']))),
        events: JSON.stringify(activeEventConsumptions.map(c => _.pick(c, ['uuid', 'eventName'])))
      },
      timeout
    }, 'Shutting down');

    await this._cancelAllConsumers();
    await this._waitForConsumersToFinish(timeout);

    // If there are still active consumers, NACK 'em all
    if (_.size(this._activeMessageConsumptions)) {
      const channel = await this._getChannel();
      await channel.nackAll();
    }

    // Close channel if open
    if (this._channel) {
      this._logger.debug({}, 'Closing channel');
      await this._channel.close();
      delete this._channel;
    }
    // Close connection if open
    if (this._conn) {
      this._logger.debug({}, 'Closing connection');
      await this._conn.close();
      delete this._conn;
    }
  }

  /**
   * Cancels all registered consumers
   *
   * @return {Promise.<void>}
   * @private
   */
  async _cancelAllConsumers() {
    const channel = await this._getChannel();

    this._logger.info({}, 'Cancelling all consumers');

    await Promise.all(this._registeredConsumers.map(c => channel.cancel(c.consumerTag)));

    this._registeredConsumers = [];
  }

  /**
   * Waits for any active message consumers to finish, optionally bounded by a timeout
   *
   * @param {int|null} timeout Timeout in milliseconds. If anything else than a positive integer, wait forever.
   * @return {Promise.<void>}
   * @private
   */
  async _waitForConsumersToFinish(timeout = null) {
    if (_.size(this._activeMessageConsumptions) === 0) {
      return;
    }

    return new Promise(resolve => {
      const resolveOnce = _.once(resolve);

      this.on('messageConsumed', (message) => {
        if (_.size(this._activeMessageConsumptions) === 0) {
          resolveOnce();
        }
      });

      if (_.isInteger(timeout) && timeout > 0) {
        setTimeout(resolveOnce, timeout);
      }
    });
  }

  /**
   * Returns the queue name to consume events for the configured service.name
   *
   * @param {string} eventKey Pattern of event(s) to consume
   * @param {string} serviceName Name of service that creates the queue
   * @return {string} Name of queue to consume events
   * @private
   */
  static _getConsumeEventQueueName(eventKey, serviceName) {
    return 'events.' + serviceName + '.' + eventKey;
  }

  /**
   * Returns the queue name to consume tasks for the configured service.name
   *
   * @param {string} taskName Name of task to consume
   * @param {string} serviceName Name of service that creates the queue
   * @return {string} Name of queue to consume tasks
   * @private
   */
  static _getregisterTaskConsumerQueueName(taskName, serviceName) {
    // Prefix with 'serviceName.' if given
    return 'tasks.' + serviceName + '.' + taskName;
  }

  /**
   * Internal function to handle the consumption of a new message
   *
   * @param {object} message Raw message object received from RabbitMQ
   * @param {string} messageType Either 'event' or 'task'
   * @param {Buffer} message.content Message content
   * @param {object} options Same options object as passed to registerEventConsumer() or registerTaskConsumer()
   * @param {string} options.queueName Name of queue that this message is consumed from
   * @param {function<Promise>} consumeFn Function that will be called for each message to consume.
   *                                      The following arguments are passed to the function:
   *                                      - {object} context Context for the message
   *                                      - {object} task/event Other message data, such as (task/event) name, uuid, time
   * @return {Promise.<*>}
   * @private
   */
  async _handleConsumeMessage(message, messageType, options, consumeFn) {
    // message === null if consumer is cancelled: http://www.rabbitmq.com/consumer-cancel.html
    if (message === null) {
      if (!options.onCancel) {
        return null;
      }

      return options.onCancel();
    }

    const channel = await this._getChannel();

    const msgObj = JSON.parse(message.content.toString());
    const context = msgObj.context;

    this._logger.debug({[messageType]: msgObj}, `${messageType} ready to be consumed`);
    this._activeMessageConsumptions.push(msgObj);

    let consumeError = null;
    try {
      const startTime = Date.now();
      const consumeResult = await consumeFn(_.cloneDeep(context), _.cloneDeep(msgObj));
      const consumeTimeMillis = Date.now() - startTime;

      const consumeResultTruncated = _.truncate(JSON.stringify(consumeResult), {length: 4096});
      this._logger.info({[messageType]: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis}, `${messageType} consumed`);
    } catch (err) {
      consumeError = err;
    }
    _.pull(this._activeMessageConsumptions, msgObj);
    this.emit('messageConsumed', msgObj);

    /*
     * ACK the message regardless of whether message consumption succeeded or not.
     * If it failed, we'll republish it to the dead letter exchange, possibly with retry
     */
    try {
      await channel.ack(message);
    } catch (err) {
      this._logger.error({[messageType]: msgObj, err}, `Error ACK\'ing ${messageType}!`);
    }

    // If no error happened during consumption, we are done here.
    if (!consumeError) {
      return true;
    }

    /*
     * If an error happened, we might want to retry at a later point in time.
     */
    return this._handleConsumeRejection(message, messageType, msgObj, consumeError, options);
  }

  /**
   * Handle a rejection from the consume function for a given message type
   *
   * @param {object} message Raw message object received from RabbitMQ
   * @param {Buffer} message.content Message content
   * @param {object} message.fields
   * @param {object} message.properties
   * @param {string} messageType Either 'event' or 'task'
   * @param {object} messageObject The raw message (task or event) object
   * @param {Error} consumeError Error that consumeFn rejected with
   * @param {object} options Same options object as passed to _handleConsumeMessage().
   * @param {function<Promise>} options.onError Function which will be called if consumeFn rejected.
   *                                            Argument to function is a single object with the following properties:
   *                                              - {Error} err The error that consumeFn rejected with
   *                                              - {object} context The message context
   *                                              - {object} event Extra event data, such as eventName, uuid, time
   *                                              OR
   *                                              - {object} task Extra task data, such as taskName, uuid, time
   *
   *                                            If this function is not given, the error will be logged with 'warn' level.
   *                                            If this function is given and rejects, the rejection will be logged with 'error' level.
   *                                            If this function resolves, nothing will be logged.
   *
   * @return {Promise.<void>}
   * @private
   */
  async _handleConsumeRejection(message, messageType, messageObject, consumeError, options) {
    const allowedTypes = ['event', 'task'];

    if (!allowedTypes.includes(messageType)) {
      throw new Error(`Invalid type. Given: ${messageType}, allowed: [${allowedTypes.join(', ')}]`);
    }

    const retryResponse = CoinifyRabbit._decideConsumerRetry(messageObject.attempts, _.get(options, 'retry'));
    let {shouldRetry} = retryResponse;
    const {delaySeconds} = retryResponse;

    // If consumeError has property noRetry: true, disregard result from _decideConsumerRetry() and never retry.
    if (consumeError && consumeError.noRetry === true) {
      shouldRetry = false;
    }

    const onError = _.has(options, 'onError') ?
      options.onError :
      async () => {
        // No onError function. We'll log it with 'warn' level
        this._logger.warn({err: consumeError, [messageType]: messageObject}, `Error consuming ${messageType}`);
      };

    // Using ES6 object literals below
    // that allow us to create a dynamic key
    // (in this case, with the value of the `type` param)
    // See: http://www.benmvp.com/learning-es6-enhanced-object-literals/
    try {
      const onErrorArgs = {
        err: consumeError,
        context: messageObject.context,
        willRetry: shouldRetry,
        delaySeconds,
        [messageType]: messageObject
      };

      await onError(onErrorArgs);
    } catch (err) {
      err.cause = consumeError;
      // Both consume function and error handling function rejected. Log as error
      this._logger.error({err, [messageType]: messageObject}, `${messageType} error handling function rejected!`);
    }

    const retryAmqpOptions = _.pick(options, ['exchange', 'queue']);

    const publishOptions = {};
    let republishExchangeName, retryQueueName;
    if (shouldRetry) {
      const retryExchangeAndQueue = await this._assertRetryExchangeAndQueue(delaySeconds, retryAmqpOptions);
      republishExchangeName = retryExchangeAndQueue.retryExchangeName;
      // For further details on the BCC header, see:
      // https://www.rabbitmq.com/sender-selected.html
      // See also:
      // http://www.squaremobius.net/amqp.node/channel_api.html#channel_publish
      publishOptions.BCC = retryExchangeAndQueue.retryQueueName;
    } else {
      // TODO: Decide whether we should have a separate method to handle task/typeObject dead letters
      republishExchangeName = await this._assertDeadLetterExchangeAndQueue(retryAmqpOptions);
    }

    messageObject = _.cloneDeep(messageObject);
    messageObject.attempts = (messageObject.attempts || 0) + 1;
    const updatedMessage = new Buffer(JSON.stringify(messageObject));

    // Publish updated message to dead letter exchange
    const channel = await this._getChannel();
    const routingKey = options.queueName;

    const publishResult = await channel.publish(republishExchangeName, routingKey, updatedMessage, publishOptions);
    if (!publishResult) {
      const err = new Error(`channel.publish() to exchange '${republishExchangeName}' with routing key '${routingKey}'`
        + ` resolved to ${JSON.stringify(publishResult)}`);
      // Add extra properties to error
      _.assign(err, {republishExchangeName, routingKey, updatedMessage});
      throw err;
    }

    // Log action
    const logContext = {[messageType]: messageObject, routingKey, shouldRetry, delaySeconds, publishOptions};
    if (shouldRetry) {
      this._logger.trace(logContext, `Scheduled ${messageType} for retry`);
    } else {
      this._logger.error(logContext, `${messageType} sent to dead-letter exchange without retry`);
    }
  }

  /**
   * Internal function to handle the consumption of a previously failed message
   *
   * @param {object} message Raw message object received from RabbitMQ
   * @param {Buffer} message.content Message content
   * @param {object} options Same options object as passed to registerEventConsumer() or registerTaskConsumer()
   * @param {string} options.onCancel function that will be called in case the consumer is cancelled
   * @param {function<Promise>} consumeFn Function that will be called for each message to consume.
   *                                      The following arguments are passed to the function:
   *                                      - {String} queueName Message origin queue
   *                                      - {object} message Full message object, currently either a task or event
   * @return {Promise.<*>}
   * @private
   */
  async _handleFailedMessage(message, options, consumeFn) {
    // message === null if consumer is cancelled: http://www.rabbitmq.com/consumer-cancel.html
    if (message === null) {
      if (!options.onCancel) {
        return null;
      }
      return options.onCancel();
    }

    const channel = await this._getChannel();

    const msgObj = JSON.parse(message.content.toString());

    this._logger.debug({message: msgObj}, 'message from failed queue ready to be consumed');
    this._activeMessageConsumptions.push(msgObj);

    try {
      const startTime = Date.now();
      const consumeResult = await consumeFn(_.cloneDeep(message.fields.routingKey), _.cloneDeep(msgObj));
      const consumeTimeMillis = Date.now() - startTime;

      const consumeResultTruncated = _.truncate(JSON.stringify(consumeResult), {length: 4096});
      await channel.ack(message);
      this._logger.info({message: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis}, 'message consumed');
    } catch (err) {
      this._logger.warn({err, message: msgObj}, 'Error consuming message from failed queue');
      await channel.nack(message);
    }
    _.pull(this._activeMessageConsumptions, msgObj);
    this.emit('messageConsumed', msgObj);
  }

  /**
   * Enqueues a message to a specific queue. Can be used for re-enqueing failed events.
   *
   * @param  {string} queueName Name of the queue to put this message
   * @param  {object} messageObject Full Event or Task message object
   * @return {object} Returns the messageObject
   */
  async enqueueMessage(queueName, messageObject, options = {}) {
    const channel = await this._getChannel();

    const message = new Buffer(JSON.stringify(messageObject));

    // Empty string is the default direct exchange
    const exchangeName = '';
    const publishResult = await channel.publish(exchangeName, queueName, message, options);

    if (!publishResult) {
      throw new Error('channel.publish() resolved to ' + JSON.stringify(publishResult));
    }

    this._logger.info({messageObject, exchangeName, options}, 'Enqueued message');

    return messageObject;
  }

  /**
   * Prepares an exchange and associated queue that will delay a message for a certain period of time,
   * and re-queue it in the main task exchange once the time is up
   *
   * @param {number} delaySeconds Delay in seconds that the exchange and queue should be configured with
   * @param {object} options Object of optional arguments
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange() for exchange
   * @param {object} options.queue Object of options to pass to amqplib's assertQueue() for queue
   * @return {Promise<{retryExchangeName, retryQueueName}>} Resolves to the name of the exchange to publish message to,
   *                                                        and the name of the retry queue with the specific delay.
   * @private
   */
  async _assertRetryExchangeAndQueue(delaySeconds, options = {}) {
    const channel = await this._getChannel();
    const delayMs = Math.round(delaySeconds * 1000);

    const retryExchangeName = _.get(this._config, 'exchanges.retry');
    const retryQueueName = _.get(this._config, 'queues.retryPrefix') + '.' + delayMs + 'ms';
    const exchangeOptions = _.defaultsDeep({}, _.get(options, 'exchange', {}), {autoDelete: true});

    await channel.assertExchange(retryExchangeName, 'direct', exchangeOptions);

    const queueOptions = _.defaultsDeep({}, _.get(options, 'queue', {}), {
      // Queue expires 3 seconds after it was declared the last time
      // expires: 3 * 1000 + delayMs,
      // autoDelete: false,
      deadLetterExchange: '', // An empty string here means that this is going to the global direct exchange
      messageTtl: delayMs
    });
    const q = await channel.assertQueue(retryQueueName, queueOptions);
    await channel.bindQueue(q.queue, retryExchangeName, q.queue);

    return {retryExchangeName, retryQueueName};
  }

  /**
   * Prepares an exchange and associated queue that will be used for depositing failed tasks
   *
   * @param {object} options Object of optional arguments
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange() for exchange
   * @param {object} options.queue Object of options to pass to amqplib's assertQueue() for queue
   * @return {Promise<string>} Resolves to name of dead letter exchange
   * @private
   */
  async _assertDeadLetterExchangeAndQueue(options) {
    const channel = await this._getChannel();

    const deadLetterExchangeName = _.get(this._config, 'exchanges.failed');
    const deadLetterQueueName = _.get(this._config, 'queues.failed');
    const exchangeOptions = _.get(options, 'exchange', {});

    await channel.assertExchange(deadLetterExchangeName, 'fanout', exchangeOptions);

    const queueOptions = _.get(options, 'queue', {});
    const q = await channel.assertQueue(deadLetterQueueName, queueOptions);
    await channel.bindQueue(q.queue, deadLetterExchangeName);

    return deadLetterExchangeName;
  }

  /**
   * Decides whether or not to retry consuming a task at a later time, along with time to delay next attempt.
   *
   * @param {number} currentAttempt Current attempt number. First failed attempt should be attempt 0
   * @param {object} options Retry options to consider
   * @param {object} options.backoff Backoff configuration for retrying tasks
   * @param {string} options.backoff.type Type of backoff: 'exponential' or 'fixed'. Defaults to 'fixed'
   *                                      For exponential backoff, the delay until next retry is calculated as (delay * (base ^ n)),
   *                                      where n is the current attempt (0-indexed). First retry is thus always after `delay` seconds
   *                                      For fixed backoff, the delay until next retry is always options.backoff.delay
   * @param {number} options.backoff.delay Delay in seconds. Defaults to 16 seconds
   * @param {number} options.backoff.base (Only for exponential backoff) The base number for the exponentiation. Defaults to 2
   * @param {number} options.maxAttempts The maximum number of retry attempts. Defaults to 12.
   *                                     If set to 1, the task will at most be run twice:
   *                                       One for the original attempt, and one retry attempt.
   *                                     Setting this to 0 is the same as setting options.retry: false
   *                                     Setting this to -1 means unlimited retries
   * @returns {{shouldRetry, delaySeconds}}
   */
  static _decideConsumerRetry(currentAttempt, options) {
    let delaySeconds = 0;

    // Check if we should retry at all
    if (!options) {
      return {shouldRetry: false, delaySeconds};
    }

    options = options || {};

    const maxAttempts = _.get(options, 'maxAttempts', 12);
    if (!_.isInteger(maxAttempts) || maxAttempts < -1) {
      throw new Error('Retry maxAttempts must be -1, 0, or a positive integer');
    }

    // Check if we have exceeded the maximum attempt counter
    if (maxAttempts !== -1 && currentAttempt >= maxAttempts) {
      return {shouldRetry: false, delaySeconds};
    }

    delaySeconds = _.get(options, 'backoff.delay', 16);
    if (!_.isNumber(delaySeconds) || delaySeconds < 0) {
      throw new Error('Retry: backoff.delay must be a strictly positive number');
    }

    // Then, it's time to compute the delay until retry
    const backoffType = _.get(options, 'backoff.type', 'fixed');
    switch (backoffType) {
      case 'exponential': {
        const eBase = _.get(options, 'backoff.base', 2);
        delaySeconds = delaySeconds * (eBase ** currentAttempt);
        break;
      }
      case 'fixed': {
        // Nothing to do here. delaySeconds is already set
        break;
      }
      default:
        throw new Error(`Retry: invalid backoff.type: '${backoffType}'`);
    }

    return {shouldRetry: true, delaySeconds};
  }

  /**
   * Validates retry task options
   *
   * @param {object} options Retry options to consider
   * @param {object} options.backoff Backoff configuration for retrying tasks
   * @param {string} options.backoff.type Type of backoff: 'exponential' or 'fixed'. Defaults to 'fixed'
   *                                      For exponential backoff, the delay until next retry is calculated as (delay * (base ^ n)),
   *                                      where n is the current attempt (0-indexed). First retry is thus always after `delay` seconds
   *                                      For fixed backoff, the delay until next retry is always options.backoff.delay
   * @param {number} options.backoff.delay Delay in seconds. Defaults to 16 seconds
   * @param {number} options.backoff.base (Only for exponential backoff) The base number for the exponentiation. Defaults to 2
   * @param {number} options.maxAttempts The maximum number of retry attempts. Defaults to 12.
   *                                     If set to 1, the task will at most be run twice:
   *                                       One for the original attempt, and one retry attempt.
   *                                     Setting this to 0 is the same as setting options.retry: false
   * @return {Promise<void>} Resolves on valid options, rejects with error message on invalid options
   */
  static async validateConsumerRetryOptions(options) {
    await CoinifyRabbit._decideConsumerRetry(0, options);
  }
}

util.inherits(CoinifyRabbit, EventEmitter);

module.exports = CoinifyRabbit;
