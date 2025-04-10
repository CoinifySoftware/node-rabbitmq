/* eslint-disable @typescript-eslint/member-ordering */

import cloneDeep from 'lodash.clonedeep';
import defaultsDeep from 'lodash.defaultsdeep';
import truncate from 'lodash.truncate';
import * as amqplib from 'amqplib';
import backoff from 'backoff';
import consoleLogLevel from 'console-log-level';
import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { EventEmitter } from 'events';
import CoinifyRabbitConfiguration, { CoinifyRabbitConnectionConfiguration, DEFAULT_CONFIGURATION } from './CoinifyRabbitConfiguration';
import Logger from './interfaces/Logger';
import DeepPartial from './DeepPartial';
import { EnqueueMessageOptions, RetryConfiguration } from './types';
import Event, { EventConsumer, EventConsumerFunction, OnEventErrorFunctionParams, RegisterEventConsumerOptions } from './messageTypes/Event';
import Task, { EnqueueTaskOptions, OnTaskErrorFunctionParams, RegisterTaskConsumerOptions, TaskConsumer, TaskConsumerFunction } from './messageTypes/Task';
import { FailedMessage, FailedMessageConsumer, FailedMessageConsumerFunction, RegisterFailedMessageConsumerOptions } from './messageTypes/FailedMessage';
import ChannelPool, { ChannelType, isConfirmChannel } from './ChannelPool';
import { promisify } from 'util';

export interface CoinifyRabbitConstructorOptions extends DeepPartial<CoinifyRabbitConfiguration> {
  logger?: Logger;
}

type Consumer = EventConsumer | TaskConsumer | FailedMessageConsumer;
type ConsumerFunction<Context = any> = EventConsumerFunction<Context> | TaskConsumerFunction<Context> | FailedMessageConsumerFunction;
type RegisterConsumerOptions = RegisterEventConsumerOptions | RegisterTaskConsumerOptions;

type ConsumeMessageOptions = RegisterConsumerOptions & {
  queueName: string;
};

export default class CoinifyRabbit extends EventEmitter {
  private config: CoinifyRabbitConfiguration;
  private logger: Logger;
  private channels: ChannelPool;
  private consumers: Consumer[] = [];
  private activeMessageConsumptions: (Event | Task | FailedMessage)[] = [];

  private isShuttingDown = false;

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
  constructor(options?: CoinifyRabbitConstructorOptions) {
    super();

    const { logger, ...config } = options ?? {};
    this.config = defaultsDeep({}, config, DEFAULT_CONFIGURATION);

    this.logger = logger ?? (consoleLogLevel({ level: this.config.defaultLogLevel }) as Logger);

    this.channels = new ChannelPool(
      this.logger,
      () => this._getConnection(),
      (channel, type) => this._onChannelOpened(channel, type),
      (type, err) => Promise.resolve(this._onChannelClosed(type, err))
    );

    if (!this.config.service.name) {
      throw new Error('options.service.name must be set');
    }
  }

  /**
   * Emit an event to the global event topic exchange
   *
   * The full event name is used as the routing key
   */
  async emitEvent(eventName: string, context: unknown, options?: EnqueueMessageOptions): Promise<Event> {
    const serviceName = options?.service?.name ?? this.config.service.name;
    const usePublisherConfirm = options?.usePublisherConfirm ?? this.config.usePublisherConfirm;

    // Prefix with service name and a dot to get full event name
    const fullEventName = serviceName ? serviceName + '.' + eventName : eventName;

    this.logger.trace({ fullEventName, context, options }, 'emitEvent()');
    const channel = await this.channels.getPublisherChannel(usePublisherConfirm);

    const exchangeName = this.config.exchanges.eventsTopic;
    await channel.assertExchange(exchangeName, 'topic', options?.exchange);

    const event: Event = {
      eventName: fullEventName,
      context,
      uuid: options?.uuid ?? uuidv4(),
      time: options?.time ? new Date(options.time).getTime() : Date.now(),
      attempts: 0
    };

    const message = Buffer.from(JSON.stringify(event));

    await this.publishMessage(channel, exchangeName, fullEventName, message);

    this.logger.info({ event, exchangeName, options }, `Event ${event.eventName} emitted`);

    return event;
  }

  /**
   * Register a consumer for an event.
   *
   * The configuration variable service.name decides the name of the queue to consume from:
   *   Consumers with the same service.name will consume from the same queue (each event will be consumed once).
   *   Consumers with different service.name will consume from different queues (each event will be consumed once per service.name)
   *
   * @returns Consumer tag
   */
  async registerEventConsumer<Context = any>(eventKey: string, consumeFn: EventConsumerFunction<Context>, options?: RegisterEventConsumerOptions): Promise<string> {
    CoinifyRabbit.validateConsumerRetryOptions(options?.retry);

    const serviceName = options?.service?.name ?? this.config.service.name;
    const exchangeName = this.config.exchanges.eventsTopic;
    const { uniqueQueue = false } = options ?? {};

    const eventQueueName = this._getConsumeEventQueueName(eventKey, serviceName, uniqueQueue);

    const consumeMessageOptions = { ...options, queueName: eventQueueName };
    this.logger.trace({ eventKey, eventQueueName }, 'registerEventConsumer()');

    const channel = await this.channels.getConsumerChannel();

    await channel.assertExchange(exchangeName, 'topic', options?.exchange);

    const queueOptions = { ...options?.queue };
    if (uniqueQueue) {
      queueOptions.autoDelete = true;
    }

    if (this.config.queues.useQuorumQueues && queueOptions.durable !== false && queueOptions.autoDelete !== true && queueOptions.exclusive !== true) {
      queueOptions.arguments = {
        ...queueOptions?.arguments,
        'x-queue-type': 'quorum'
      };
    }

    const q = await channel.assertQueue(eventQueueName, queueOptions);
    await channel.bindQueue(q.queue, exchangeName, eventKey);

    const prefetch = options?.consumer?.prefetch ?? this.config.consumer.prefetch;
    await channel.prefetch(prefetch, false);
    const { consumerTag } = await channel.consume(q.queue,
      (message) => this._handleConsumeMessage(message, 'event', consumeMessageOptions, consumeFn),
      { consumerTag: options?.consumerTag }
    );

    this.consumers.push({ type: 'event', key: eventKey, consumerTag, consumeFn, options });

    return consumerTag;
  }

  /**
   * Enqueue a task using the global task topic exchange
   *
   * The full task name is used as the routing key
   *   */
  async enqueueTask(fullTaskName: string, context: unknown, options?: EnqueueTaskOptions) {
    const serviceName = options?.service?.name ?? this.config.service.name;
    const usePublisherConfirm = options?.usePublisherConfirm ?? this.config.usePublisherConfirm;

    const channel = await this.channels.getPublisherChannel(usePublisherConfirm);
    const delayMillis = options?.delayMillis ?? 0;

    let exchangeName;
    const publishOptions: amqplib.Options.Publish = {};
    if (delayMillis > 0) {
      const delayedAmqpOptions = { exchange: options?.exchange };
      const { delayedExchangeName, delayedQueueName } = await this._assertDelayedTaskExchangeAndQueue(channel, delayMillis, delayedAmqpOptions);
      exchangeName = delayedExchangeName;
      publishOptions.BCC = delayedQueueName;
    } else {
      exchangeName = this.config.exchanges.tasksTopic;
      await channel.assertExchange(exchangeName, 'topic', options?.exchange);
    }

    this.logger.trace({ fullTaskName, context, exchangeName, options, publishOptions }, 'enqueueTask()');

    const task: Task = {
      taskName: fullTaskName,
      context,
      uuid: options?.uuid ?? uuidv4(),
      time: options?.time ? new Date(options.time).getTime() : Date.now(),
      attempts: 0,
      origin: serviceName,
      delayMillis: delayMillis > 0 ? delayMillis : undefined
    };

    const message = Buffer.from(JSON.stringify(task));

    await this.publishMessage(channel, exchangeName, fullTaskName, message, publishOptions);

    this.logger.info({ task, exchangeName, options }, `Task ${task.taskName} enqueued`);

    return task;
  }

  /**
   * Register a consumer for a task.
   *
   * The configuration variable service.name decides the name of the queue to consume from:
   *   For e.g. a taskName of 'my-task' and a service.name of 'my-service', the task queue will be 'my-service.my-task'
   *
   * @returns Consumer tag
   */
  async registerTaskConsumer<Context = any>(taskName: string, consumeFn: TaskConsumerFunction<Context>, options?: RegisterTaskConsumerOptions) {
    CoinifyRabbit.validateConsumerRetryOptions(options?.retry);

    const serviceName = options?.service?.name ?? this.config.service.name;
    const exchangeName = this.config.exchanges.tasksTopic;
    const { uniqueQueue = false } = options ?? {};

    const fullTaskName = serviceName + '.' + taskName;
    const taskQueueName = this._getTaskConsumerQueueName(taskName, serviceName, uniqueQueue);

    const consumeMessageOptions = { ...options, queueName: taskQueueName };
    this.logger.trace({ taskName, fullTaskName, taskQueueName, exchangeName, options }, 'registerTaskConsumer()');

    const channel = await this.channels.getConsumerChannel();

    await channel.assertExchange(exchangeName, 'topic', options?.exchange);

    const queueOptions = { ...options?.queue };
    if (uniqueQueue) {
      queueOptions.autoDelete = true;
    }

    if (this.config.queues.useQuorumQueues && queueOptions.durable !== false && queueOptions.autoDelete !== true && queueOptions.exclusive !== true) {
      queueOptions.arguments = {
        ...queueOptions?.arguments,
        'x-queue-type': 'quorum'
      };
    }
    const q = await channel.assertQueue(taskQueueName, queueOptions);
    await channel.bindQueue(q.queue, exchangeName, fullTaskName);

    const prefetch = options?.consumer?.prefetch ?? this.config.consumer.prefetch;
    await channel.prefetch(prefetch, false);
    const { consumerTag } = await channel.consume(q.queue,
      (message) => this._handleConsumeMessage(message, 'task', consumeMessageOptions, consumeFn),
      { consumerTag: options?.consumerTag }
    );

    this.consumers.push({ type: 'task', key: taskName, consumerTag, consumeFn, options });

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
   *                                        This defaults to true if not supplied
   * @param {boolean} options.queue.autoDelete If true, the queue will be destroyed once the number of consumers drops
   *                                           to zero. Defaults to false.
   * @param {number} options.consumer.prefetch Sets the limit for number of unacknowledged messages for this consumer.
   *                                           Defaults to the consumer.prefetch configuration value.
   *                                           If this option is specified, all calls to this function (and registerEventConsumer)
   *                                           must be done serially to avoid race conditions, causing incorrect prefetch values to be set
   * @return {Promise<string>} Returns the consumer tag which is needed to cancel the consumer
   */
  async registerFailedMessageConsumer(consumeFn: FailedMessageConsumerFunction, options?: RegisterFailedMessageConsumerOptions) {
    const channel = await this.channels.getConsumerChannel();
    const queueName = this.config.queues.failed;
    this.logger.trace({ queueName }, 'registerFailedMessageConsumer()');

    const queueOptions = { ...options?.queue };
    if (this.config.queues.useQuorumQueues && queueOptions.durable !== false && queueOptions.autoDelete !== true && queueOptions.exclusive !== true) {
      queueOptions.arguments = {
        ...queueOptions?.arguments,
        'x-queue-type': 'quorum'
      };
    }
    const q = await channel.assertQueue(queueName, queueOptions);
    const prefetch = options?.consumer?.prefetch ?? this.config.consumer.prefetch;
    await channel.prefetch(prefetch, false);
    const { consumerTag } = await channel.consume(q.queue,
      (message) => message && this._handleFailedMessage(message, { ...options, queueName }, consumeFn),
      { consumerTag: options?.consumerTag }
    );

    this.consumers.push({ type: 'message', key: 'failed', consumerTag, consumeFn, options });

    return consumerTag;
  }

  async assertConnection() {
    await this._getConnection();
  }

  private _conn?: amqplib.ChannelModel;
  private _getConnectionPromise?: Promise<void>;

  /**
   * Get a singleton connection to RabbitMQ
   * @return {Promise<amqplib.Connection>}
   * @private
   */
  async _getConnection(): Promise<amqplib.ChannelModel> {
    if (!this._conn) {
      if (this.isShuttingDown) {
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
            const connectionConfig = this.config.connection;
            this.logger.info({ connectionConfig: { ...connectionConfig, password: undefined } }, 'Opening connection to RabbitMQ');

            this._conn = await amqplib.connect(CoinifyRabbit._generateConnectionUrl(connectionConfig), { clientProperties: { connection_name: this.config.service.name } });
            this._conn.on('error', err => {
              // Basic error handling: Log error and discard current connection, so a new one will be created next time
              this.logger.warn({ err }, 'RabbitMQ connection error');
              delete this._conn;
            });
            this._conn.on('close', err => {
              delete this._conn;
              this.logger.info({ err }, 'RabbitMQ Connection closed');
            });

            await this.onConnectionOpened();
          } catch (err) {
            this.logger.error({ err }, 'Error connecting to RabbitMQ');
          } finally {
            delete this._getConnectionPromise;
          }
        })();
      }

      await this._getConnectionPromise;
    }

    if (!this._conn) {
      throw new Error('Could not create connection to RabbitMQ');
    }

    return this._conn;
  }

  private async onConnectionOpened(): Promise<void> {
    this.logger.info({}, 'RabbitMQ Connection opened');

    if (this.consumers.length) {
      await this._recreateRegisteredConsumers();
    }
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
  static _generateConnectionUrl(connectionConfig: CoinifyRabbitConnectionConfiguration) {
    // Check for valid protocol
    if (![ 'amqp', 'amqps' ].includes(connectionConfig.protocol)) {
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
  private async _onChannelOpened(channel: amqplib.Channel, type: ChannelType) {
    this.logger.info({ type }, `RabbitMQ ${type} channel opened`);

    if (type === 'consumer') {
      const prefetch = this.config.channel.prefetch;
      await channel.prefetch(prefetch, true);
    }
  }

  /**
   * Re-attach event and task consumers to their respective queues
   *
   * @return {Promise.<void>}
   * @private
   */
  private async _recreateRegisteredConsumers() {
    const consumers: Consumer[] = cloneDeep(this.consumers);
    this.consumers = [];

    this.logger.info({
      consumers: consumers.map(c => ({ type: c.type, key: c.key, tag: c.consumerTag }))
    }, `Recreating ${consumers.length} registered consumers`);

    /*
     * Re-create all registered consumers
     */
    for (const consumer of consumers) {
      switch (consumer.type) {
        case 'event':
          await this.registerEventConsumer(consumer.key, consumer.consumeFn, { ...consumer.options, consumerTag: consumer.consumerTag });
          break;
        case 'task':
          await this.registerTaskConsumer(consumer.key, consumer.consumeFn, { ...consumer.options, consumerTag: consumer.consumerTag });
          break;
        case 'message':
          await this.registerFailedMessageConsumer(consumer.consumeFn, { ...consumer.options, consumerTag: consumer.consumerTag });
          break;
        default:
          throw new Error(`Internal error: Unknown consumer type '${(consumer as any).type}'`);
      }
    }
  }

  /**
   * Function that is run when a channel closes..
   *
   * If channel was requested closed by calling #close(), no action is taken.
   * Otherwise, if there are registered consumers, we will try to reconnect with backoff.
   *
   * @param {Error | undefined} err
   * @return {Promise.<void>}
   * @private
   */
  private _onChannelClosed(type: ChannelType, err?: Error) {
    if (this.isShuttingDown) {
      this.logger.info({ err, type }, `RabbitMQ ${type} channel closed`);
      // Channel close requested. We won't try to reconnect
      return;
    }

    this.logger.warn({ err, type }, `RabbitMQ ${type} channel closed unexpectedly: ${err ? err.message : 'No error given'}`);

    if (!this.consumers.length) {
      // No registered consumers, no need to reconnect automatically.
      return;
    }

    return this._connectWithBackoff();
  }

  private async publishMessage(channel: amqplib.Channel, exchange: string, routingKey: string, content: Buffer, options?: amqplib.Options.Publish): Promise<void> {
    const publishOptions: amqplib.Options.Publish = {
      persistent: this.config.publish.persistentMessages,
      ...options
    };

    if (isConfirmChannel(channel)) {
      await promisify(channel.publish).bind(channel)(exchange, routingKey, content, publishOptions);
    } else {
      const publishResult = channel.publish(exchange, routingKey, content, publishOptions);
      if (!publishResult) {
        this.logger.warn({
          exchange, routingKey, content: content.toString(), publishOptions
        }, `channel.publish() to exchange '${exchange}' with routing key '${routingKey}' returned false`);

        throw new Error(`channel.publish() to exchange '${exchange}' with routing key '${routingKey}' returned false`);
      }
    }
  }

  /**
   * Connects and tries to create a channel using fibonacci backoff
   *
   * @return {Promise.<void>}
   * @private
   */
  private _connectWithBackoff() {
    // Attempts to reconnect after 1, 1, 2, 3, 5, 10, 20, 30, 50, 60, 60, 60... seconds
    const fibonacciBackoff = backoff.fibonacci({
      initialDelay: 1000,
      maxDelay: 600000
    });

    fibonacciBackoff.on('ready', (number, delay) => {
      if (this.isShuttingDown) {
        return;
      }
      this.logger.info({}, `Connecting... (attempt ${number}, delay ${delay}ms)`);

      // If we're already attempting to connect, no reason to add another listener to the promise
      if (this._getConnectionPromise) {
        return;
      }

      // Try to connect to a channel
      this._getConnection()
        // If error, log and retry again later
        .catch(err => {
          fibonacciBackoff.backoff();
          this.logger.warn({ err }, 'Error reconnecting to RabbitMQ');
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
  async shutdown(timeout?: number) {
    if (this.isShuttingDown) {
      return;
    }
    this.isShuttingDown = true;

    /*
     * Log information about shutdown process
     */
    const activeTaskConsumptions = this.activeMessageConsumptions.filter((m): m is Task => 'taskName' in m);
    const activeEventConsumptions: Event[] = this.activeMessageConsumptions.filter((m): m is Event => 'eventName' in m);

    this.logger.info({
      registeredConsumers: JSON.stringify(this.consumers.map(({ type, key, consumerTag }) => ({ type, key, consumerTag }))),
      activeConsumption: {
        tasks: JSON.stringify(activeTaskConsumptions.map(({ uuid, taskName }) => ({ uuid, taskName }))),
        events: JSON.stringify(activeEventConsumptions.map(({ uuid, eventName }) => ({ uuid, eventName })))
      },
      timeout
    }, 'Shutting down RabbitMQ');

    if (this.consumers.length > 0) {
      await this._cancelAllConsumers();
      await this._waitForConsumersToFinish(timeout);
    }

    // If there are still active consumers, NACK 'em all
    if (this.activeMessageConsumptions.length > 0) {
      const channel = await this.channels.getConsumerChannel();
      channel.nackAll();
    }

    await this.channels.close();

    // Close connection if open
    if (this._conn) {
      this.logger.info({}, 'Closing connection');
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
  private async _cancelAllConsumers() {
    if (!this.channels.isChannelOpen('consumer')) {
      return;
    }

    const channel = await this.channels.getConsumerChannel();

    this.logger.info({}, 'Cancelling all consumers');

    await Promise.all(this.consumers.map(c => channel.cancel(c.consumerTag)));

    this.consumers = [];
  }

  /**
   * Waits for any active message consumers to finish, optionally bounded by a timeout
   *
   * @param {number|undefined} timeout Timeout in milliseconds. If anything else than a positive integer, wait forever.
   * @return {Promise.<void>}
   * @private
   */
  private _waitForConsumersToFinish(timeout?: number) {
    if (this.activeMessageConsumptions.length === 0) {
      return;
    }

    return new Promise(resolve => {
      let resolved = false;
      const resolveOnce = <T>(arg: T) => {
        if (!resolved) {
          resolved = true;
          resolve(arg);
        }
      };

      this.on('messageConsumed', () => {
        if (this.activeMessageConsumptions.length === 0) {
          resolveOnce(undefined);
        }
      });

      if (timeout && Number.isInteger(timeout) && timeout > 0) {
        setTimeout(resolveOnce, timeout);
      }
    });
  }

  /**
   * Returns a unique identifier for this instance
   *
   * @return {string}
   * @private
   */
  _getInstanceIdentifier() {
    return crypto.randomBytes(10).toString('hex');
  }

  /**
   * Returns the queue name to consume events for the configured service.name
   *
   * @param {string} eventKey Pattern of event(s) to consume
   * @param {string} serviceName Name of service that creates the queue
   * @param {boolean} uniqueQueue Set to true to generate unique queue name for each instance of the service
   * @return {string} Name of queue to consume events
   * @private
   */
  _getConsumeEventQueueName(eventKey: string, serviceName: string, uniqueQueue = false) {
    let queueName = 'events.' + serviceName;
    if (uniqueQueue) {
      queueName += '.instance-' + this._getInstanceIdentifier();
    }
    queueName += '.' + eventKey;
    return queueName;
  }

  /**
   * Returns the queue name to consume tasks for the configured service.name
   *
   * @param {string} taskName Name of task to consume
   * @param {string} serviceName Name of service that creates the queue
   * @param {boolean} uniqueQueue Set to true to generate unique queue name for each instance of the service
   * @return {string} Name of queue to consume tasks
   * @private
   */
  _getTaskConsumerQueueName(taskName: string, serviceName: string, uniqueQueue = false) {
    let queueName = 'tasks.' + serviceName;
    if (uniqueQueue) {
      queueName += '.instance-' + this._getInstanceIdentifier();
    }
    queueName += '.' + taskName;
    return queueName;
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
  private async _handleConsumeMessage<Context = any>(message: amqplib.ConsumeMessage | null, messageType: 'event' | 'task', options: ConsumeMessageOptions, consumeFn: ConsumerFunction<Context>) {
    // message === null if consumer is cancelled: http://www.rabbitmq.com/consumer-cancel.html
    if (message === null) {
      if (!options.onCancel) {
        return null;
      }

      return options.onCancel();
    }

    const channel = await this.channels.getConsumerChannel();

    const msgObj = JSON.parse(message.content.toString());
    const context = msgObj.context;
    const name = messageType === 'event' ? msgObj.eventName : msgObj.taskName;

    this.logger.info({ [messageType]: msgObj }, `${messageType} ${name} ready to be consumed`);
    this.activeMessageConsumptions.push(msgObj);

    let consumeError = null;
    try {
      const startTime = Date.now();
      const consumeResult = await consumeFn(cloneDeep(context), cloneDeep(msgObj));
      const consumeTimeMillis = Date.now() - startTime;

      const consumeResultTruncated = truncate(JSON.stringify(consumeResult), { length: 4096 });
      this.logger.info({ [messageType]: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis }, `${messageType} ${name} consumed`);
    } catch (err) {
      consumeError = err;
    }

    this.activeMessageConsumptions = this.activeMessageConsumptions.filter(msg => msg !== msgObj);
    this.emit('messageConsumed', msgObj);

    /*
     * ACK the message regardless of whether message consumption succeeded or not.
     * If it failed, we'll republish it to the dead letter exchange, possibly with retry
     */
    try {
      channel.ack(message);
    } catch (err) {
      this.logger.error({ [messageType]: msgObj, err }, `Error ACK\'ing ${messageType}!`);
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
   *                                            If this function is not given, the error will be logged with 'error' level.
   *                                            If this function is given and rejects, the rejection will be logged with 'error' level.
   *                                            If this function resolves, nothing will be logged.
   *
   * @return {Promise.<void>}
   * @private
   */
  private async _handleConsumeRejection(message: amqplib.ConsumeMessage, messageType: 'event' | 'task', messageObject: Event | Task, consumeError: any, options: ConsumeMessageOptions) {
    const allowedTypes = [ 'event', 'task' ];

    if (!allowedTypes.includes(messageType)) {
      throw new Error(`Invalid type. Given: ${messageType}, allowed: [${allowedTypes.join(', ')}]`);
    }

    const retryResponse = CoinifyRabbit._decideConsumerRetry(messageObject.attempts, options.retry);
    let { shouldRetry } = retryResponse;
    const { delaySeconds } = retryResponse;

    // If consumeError has property noRetry: true, disregard result from _decideConsumerRetry() and never retry.
    if (consumeError && consumeError.noRetry === true) {
      shouldRetry = false;
    }

    const messageName = 'eventName' in messageObject ? messageObject.eventName : messageObject.taskName;

    const onError = options?.onError ??
      (() => {
        // No onError function. We'll just log it with 'error' level
        const errMessage = `Error consuming ${messageType} ${messageName}: ${consumeError.message}. `
          + (shouldRetry ? `Will retry in ${delaySeconds} seconds` : 'No retry');
        if (shouldRetry) {
          this.logger.info({ err: consumeError, [messageType]: messageObject }, errMessage);
        } else {
          this.logger.error({ err: consumeError, [messageType]: messageObject }, errMessage);
        }
      });

    // Using ES6 object literals below
    // that allow us to create a dynamic key
    // (in this case, with the value of the `type` param)
    // See: http://www.benmvp.com/learning-es6-enhanced-object-literals/
    try {
      const onErrorArgs: OnEventErrorFunctionParams & OnTaskErrorFunctionParams = {
        err: consumeError,
        context: messageObject.context,
        willRetry: shouldRetry,
        delaySeconds,
        event: 'eventName' in messageObject ? messageObject : undefined,
        task: 'taskName' in messageObject ? messageObject : undefined
      } as any;

      await onError(onErrorArgs);
    } catch (err) {
      (err as any).cause = consumeError;
      // Both consume function and error handling function rejected. Log as error
      this.logger.error({ err, [messageType]: messageObject }, `${messageType} error handling function rejected!`);
    }

    const retryAmqpOptions = { exchange: options.exchange, queue: options.queue };

    const publishOptions: amqplib.Options.Publish = {};
    const channel = await this.channels.getPublisherChannel(this.config.usePublisherConfirm);
    let republishExchangeName;
    if (shouldRetry) {
      const retryExchangeAndQueue = await this._assertRetryExchangeAndQueue(channel, delaySeconds, retryAmqpOptions);
      republishExchangeName = retryExchangeAndQueue.retryExchangeName;
      // For further details on the BCC header, see:
      // https://www.rabbitmq.com/sender-selected.html
      // See also:
      // http://www.squaremobius.net/amqp.node/channel_api.html#channel_publish
      publishOptions.BCC = retryExchangeAndQueue.retryQueueName;
    } else {
      // TODO: Decide whether we should have a separate method to handle task/typeObject dead letters
      republishExchangeName = await this._assertDeadLetterExchangeAndQueue(channel, retryAmqpOptions);
    }

    messageObject = cloneDeep(messageObject);
    messageObject.attempts = (messageObject.attempts || 0) + 1;
    const updatedMessage = Buffer.from(JSON.stringify(messageObject));

    // Publish updated message to dead letter exchange
    const routingKey = options.queueName;
    await this.publishMessage(channel, republishExchangeName, routingKey, updatedMessage, publishOptions);

    // Log action
    const logContext = { [messageType]: messageObject, routingKey, shouldRetry, delaySeconds, publishOptions };
    if (shouldRetry) {
      this.logger.trace(logContext, `Scheduled ${messageType} for retry`);
    } else {
      this.logger.info(logContext, `${messageType} ${messageName} sent to dead-letter exchange without retry`);
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
  private async _handleFailedMessage(message: amqplib.ConsumeMessage, options: ConsumeMessageOptions, consumeFn: ConsumerFunction) {
    // message === null if consumer is cancelled: http://www.rabbitmq.com/consumer-cancel.html
    if (message === null) {
      if (!options.onCancel) {
        return null;
      }
      return options.onCancel();
    }

    const channel = await this.channels.getConsumerChannel();

    const msgObj = JSON.parse(message.content.toString());

    this.logger.debug({ message: msgObj }, 'message from failed queue ready to be consumed');
    this.activeMessageConsumptions.push(msgObj);

    try {
      const startTime = Date.now();
      const consumeResult = await consumeFn(cloneDeep(message.fields.routingKey), cloneDeep(msgObj));
      const consumeTimeMillis = Date.now() - startTime;

      const consumeResultTruncated = truncate(JSON.stringify(consumeResult), { length: 4096 });
      channel.ack(message);
      this.logger.info({ message: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis }, 'message consumed');
    } catch (err) {
      this.logger.warn({ err, message: msgObj }, 'Error consuming message from failed queue');
      channel.nack(message);
    }
    this.activeMessageConsumptions = this.activeMessageConsumptions.filter(msg => msg !== msgObj);
    this.emit('messageConsumed', msgObj);
  }

  /**
   * Enqueues a message to a specific queue. Can be used for re-enqueing failed events.
   *
   * @param  {string} queueName Name of the queue to put this message
   * @param  {object} messageObject Full Event or Task message object
   * @param  {object} options Object of options to pass to amqplib's channel.publish()
   * @return {object} Returns the messageObject
   */
  async enqueueMessage(queueName: string, messageObject: Event | Task, options?: { exchange?: amqplib.Options.Publish; usePublisherConfirm?: boolean }) {
    const usePublisherConfirm = options?.usePublisherConfirm ?? this.config.usePublisherConfirm;
    const channel = await this.channels.getPublisherChannel(usePublisherConfirm);

    const message = Buffer.from(JSON.stringify(messageObject));

    // Empty string is the default direct exchange
    const exchangeName = '';
    await this.publishMessage(channel, exchangeName, queueName, message, options?.exchange);

    this.logger.info({ messageObject, exchangeName, options }, 'Enqueued message');

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
  private async _assertRetryExchangeAndQueue(channel: amqplib.Channel, delaySeconds: number, options?: { exchange?: amqplib.Options.AssertExchange; queue?: amqplib.Options.AssertQueue }) {
    const delayMs = Math.round(delaySeconds * 1000);

    const retryExchangeName = this.config.exchanges.retry;
    const retryQueueName = this.config.queues.retryPrefix + '.' + delayMs + 'ms';
    const exchangeOptions = defaultsDeep({}, options?.exchange, { autoDelete: true });

    await channel.assertExchange(retryExchangeName, 'direct', exchangeOptions);

    const queueOptions = defaultsDeep({}, options?.queue, {
      // Queue expires 3 seconds after it was declared the last time
      // expires: 3 * 1000 + delayMs,
      // autoDelete: false,
      deadLetterExchange: '', // An empty string here means that this is going to the global direct exchange
      messageTtl: delayMs
    });

    if (this.config.queues.useQuorumQueues && queueOptions.durable !== false && queueOptions.autoDelete !== true && queueOptions.exclusive !== true) {
      queueOptions.arguments = {
        ...queueOptions?.arguments,
        'x-queue-type': 'quorum'
      };
    }

    const q = await channel.assertQueue(retryQueueName, queueOptions);
    await channel.bindQueue(q.queue, retryExchangeName, q.queue);

    return { retryExchangeName, retryQueueName };
  }

  /**
   * Prepares an exchange and associated queue that will delay a message for a certain period of time,
   * and re-queue it in the main task exchange once the time is up
   *
   * @param {number} delayMillis Delay in milliseconds that the exchange and queue should be configured with
   * @param {object} options Object of optional arguments
   * @param {object} options.exchange Object of options to pass to amqplib's assertExchange() for exchange
   * @param {object} options.queue Object of options to pass to amqplib's assertQueue() for queue
   * @return {Promise<{delayedExchangeName, delayedQueueName}>} Resolves to the name of the exchange to publish message to,
   *                                                        and the name of the retry queue with the specific delay.
   * @private
   */
  private async _assertDelayedTaskExchangeAndQueue(channel: amqplib.Channel, delayMillis: number, options?: { exchange?: amqplib.Options.AssertExchange; queue?: amqplib.Options.AssertQueue }) {
    const delayedExchangeName = this.config.exchanges.delayed;
    const delayedQueueName = this.config.queues.delayedTaskPrefix + '.' + delayMillis + 'ms';
    const exchangeOptions: amqplib.Options.AssertExchange = { ...options?.exchange, autoDelete: true };
    const tasksExchangeName = this.config.exchanges.tasksTopic;

    await channel.assertExchange(delayedExchangeName, 'direct', exchangeOptions);

    const queueOptions: amqplib.Options.AssertQueue = {
      ...options?.queue,
      expires: delayMillis + 10000,
      autoDelete: true,
      deadLetterExchange: tasksExchangeName,
      messageTtl: delayMillis
    };

    if (this.config.queues.useQuorumQueues && queueOptions.durable !== false && queueOptions.autoDelete !== true && queueOptions.exclusive !== true) {
      queueOptions.arguments = {
        ...queueOptions?.arguments,
        'x-queue-type': 'quorum'
      };
    }
    const q = await channel.assertQueue(delayedQueueName, queueOptions);
    await channel.bindQueue(q.queue, delayedExchangeName, q.queue);

    return { delayedExchangeName, delayedQueueName };
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
  private async _assertDeadLetterExchangeAndQueue(channel: amqplib.Channel, options?: { exchange?: amqplib.Options.AssertExchange; queue?: amqplib.Options.AssertQueue }) {
    const deadLetterExchangeName = this.config.exchanges.failed;
    const deadLetterQueueName = this.config.queues.failed;

    await channel.assertExchange(deadLetterExchangeName, 'fanout', options?.exchange);

    const queueOptions = { ...options?.queue };
    if (this.config.queues.useQuorumQueues && queueOptions.durable !== false && queueOptions.autoDelete !== true && queueOptions.exclusive !== true) {
      queueOptions.arguments = {
        ...queueOptions?.arguments,
        'x-queue-type': 'quorum'
      };
    }
    const q = await channel.assertQueue(deadLetterQueueName, queueOptions);
    // TODO Does '' below work?
    await channel.bindQueue(q.queue, deadLetterExchangeName, '');

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
  static _decideConsumerRetry(currentAttempt: number, options?: RetryConfiguration): { shouldRetry: boolean; delaySeconds: number } {
    let delaySeconds = 0;

    // Check if we should retry at all
    if (!options) {
      return { shouldRetry: false, delaySeconds };
    }

    const maxAttempts = options.maxAttempts ?? 12;
    if (!Number.isInteger(maxAttempts) || maxAttempts < -1) {
      throw new Error('Retry maxAttempts must be -1, 0, or a positive integer');
    }

    // Check if we have exceeded the maximum attempt counter
    if (maxAttempts !== -1 && currentAttempt >= maxAttempts) {
      return { shouldRetry: false, delaySeconds };
    }

    delaySeconds = options.backoff?.delay ?? 16;
    if (!Number.isFinite(delaySeconds) || delaySeconds < 0) {
      throw new Error('Retry: backoff.delay must be a strictly positive number');
    }

    // Then, it's time to compute the delay until retry
    switch (options.backoff?.type) {
      case 'exponential': {
        const eBase = options.backoff?.base ?? 2;
        delaySeconds = delaySeconds * eBase ** currentAttempt;
        break;
      }
      case undefined:
      case 'fixed':
        // Nothing to do here. delaySeconds is already set
        break;
      default:
        throw new Error(`Retry: invalid backoff.type: '${(options.backoff as any)?.type}'`);
    }

    return { shouldRetry: true, delaySeconds };
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
  static validateConsumerRetryOptions(options?: RetryConfiguration) {
    CoinifyRabbit._decideConsumerRetry(0, options);
  }
}
