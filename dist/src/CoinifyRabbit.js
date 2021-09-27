'use strict';
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const amqplib_1 = __importDefault(require("amqplib"));
const backoff_1 = __importDefault(require("backoff"));
const console_log_level_1 = __importDefault(require("console-log-level"));
const crypto_1 = __importDefault(require("crypto"));
const uuid_1 = require("uuid");
const util_1 = __importDefault(require("util"));
const events_1 = __importDefault(require("events"));
const CoinifyRabbitConfiguration_1 = require("./CoinifyRabbitConfiguration");
class CoinifyRabbit extends events_1.default {
    constructor(options) {
        super();
        this.consumers = [];
        this.activeMessageConsumptions = [];
        this.isShuttingDown = false;
        const { logger, ...config } = options !== null && options !== void 0 ? options : {};
        this.config = lodash_1.default.defaultsDeep({}, config, CoinifyRabbitConfiguration_1.DEFAULT_CONFIGURATION);
        this.logger = logger !== null && logger !== void 0 ? logger : (0, console_log_level_1.default)({ level: this.config.defaultLogLevel });
        if (!this.config.service.name) {
            throw new Error('options.service.name must be set');
        }
    }
    async emitEvent(eventName, context, options) {
        var _a, _b, _c;
        const serviceName = (_b = (_a = options === null || options === void 0 ? void 0 : options.service) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : this.config.service.name;
        const fullEventName = serviceName ? serviceName + '.' + eventName : eventName;
        this.logger.trace({ fullEventName, context, options }, 'emitEvent()');
        const channel = await this._getChannel();
        const exchangeName = this.config.exchanges.eventsTopic;
        await channel.assertExchange(exchangeName, 'topic', options === null || options === void 0 ? void 0 : options.exchange);
        const event = {
            eventName: fullEventName,
            context,
            uuid: (_c = options === null || options === void 0 ? void 0 : options.uuid) !== null && _c !== void 0 ? _c : (0, uuid_1.v4)(),
            time: (options === null || options === void 0 ? void 0 : options.time) ? new Date(options.time).getTime() : Date.now(),
            attempts: 0
        };
        const message = Buffer.from(JSON.stringify(event));
        const publishResult = channel.publish(exchangeName, fullEventName, message);
        if (!publishResult) {
            throw new Error('channel.publish() resolved to ' + JSON.stringify(publishResult));
        }
        this.logger.info({ event, exchangeName, options }, 'Event emitted');
        return event;
    }
    async registerEventConsumer(eventKey, consumeFn, options) {
        var _a, _b, _c, _d;
        CoinifyRabbit.validateConsumerRetryOptions(options === null || options === void 0 ? void 0 : options.retry);
        const serviceName = (_b = (_a = options === null || options === void 0 ? void 0 : options.service) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : this.config.service.name;
        const exchangeName = this.config.exchanges.eventsTopic;
        const { uniqueQueue = false } = options !== null && options !== void 0 ? options : {};
        const eventQueueName = this._getConsumeEventQueueName(eventKey, serviceName, uniqueQueue);
        const consumeMessageOptions = { ...options, queueName: eventQueueName };
        this.logger.trace({ eventKey, eventQueueName }, 'registerEventConsumer()');
        const channel = await this._getChannel();
        await channel.assertExchange(exchangeName, 'topic', options === null || options === void 0 ? void 0 : options.exchange);
        const queueOptions = { ...options === null || options === void 0 ? void 0 : options.queue };
        if (uniqueQueue) {
            queueOptions.autoDelete = true;
        }
        const q = await channel.assertQueue(eventQueueName, queueOptions);
        await channel.bindQueue(q.queue, exchangeName, eventKey);
        const prefetch = (_d = (_c = options === null || options === void 0 ? void 0 : options.consumer) === null || _c === void 0 ? void 0 : _c.prefetch) !== null && _d !== void 0 ? _d : this.config.consumer.prefetch;
        await channel.prefetch(prefetch, false);
        const { consumerTag } = await channel.consume(q.queue, (message) => this._handleConsumeMessage(message, 'event', consumeMessageOptions, consumeFn), { consumerTag: options === null || options === void 0 ? void 0 : options.consumerTag });
        this.consumers.push({ type: 'event', key: eventKey, consumerTag, consumeFn, options });
        return consumerTag;
    }
    async enqueueTask(fullTaskName, context, options) {
        var _a, _b, _c;
        const serviceName = (_b = (_a = options === null || options === void 0 ? void 0 : options.service) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : this.config.service.name;
        const channel = await this._getChannel();
        const delayMillis = lodash_1.default.get(options, 'delayMillis', 0);
        let exchangeName;
        const publishOptions = {};
        if (delayMillis > 0) {
            const delayedAmqpOptions = lodash_1.default.pick(options, ['exchange', 'queue']);
            const { delayedExchangeName, delayedQueueName } = await this._assertDelayedTaskExchangeAndQueue(delayMillis, delayedAmqpOptions);
            exchangeName = delayedExchangeName;
            publishOptions.BCC = delayedQueueName;
        }
        else {
            exchangeName = lodash_1.default.get(this.config, 'exchanges.tasksTopic');
            await channel.assertExchange(exchangeName, 'topic', lodash_1.default.get(options, 'exchange', {}));
        }
        this.logger.trace({ fullTaskName, context, exchangeName, options, publishOptions }, 'enqueueTask()');
        const task = {
            taskName: fullTaskName,
            context,
            uuid: (_c = options === null || options === void 0 ? void 0 : options.uuid) !== null && _c !== void 0 ? _c : (0, uuid_1.v4)(),
            time: (options === null || options === void 0 ? void 0 : options.time) ? new Date(options.time).getTime() : Date.now(),
            attempts: 0,
            origin: serviceName,
            delayMillis: delayMillis > 0 && delayMillis
        };
        const message = Buffer.from(JSON.stringify(task));
        const publishResult = channel.publish(exchangeName, fullTaskName, message, publishOptions);
        if (!publishResult) {
            throw new Error('channel.publish() resolved to ' + JSON.stringify(publishResult));
        }
        this.logger.info({ task, exchangeName, options }, 'Enqueued task');
        return task;
    }
    async registerTaskConsumer(taskName, consumeFn, options) {
        var _a, _b, _c, _d;
        CoinifyRabbit.validateConsumerRetryOptions(options === null || options === void 0 ? void 0 : options.retry);
        const serviceName = (_b = (_a = options === null || options === void 0 ? void 0 : options.service) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : this.config.service.name;
        const exchangeName = this.config.exchanges.tasksTopic;
        const { uniqueQueue = false } = options !== null && options !== void 0 ? options : {};
        const fullTaskName = serviceName + '.' + taskName;
        const taskQueueName = this._getTaskConsumerQueueName(taskName, serviceName, uniqueQueue);
        const consumeMessageOptions = { ...options, queueName: taskQueueName };
        this.logger.trace({ taskName, fullTaskName, taskQueueName, exchangeName, options }, 'registerTaskConsumer()');
        const channel = await this._getChannel();
        await channel.assertExchange(exchangeName, 'topic', options === null || options === void 0 ? void 0 : options.exchange);
        const queueOptions = { ...options === null || options === void 0 ? void 0 : options.queue };
        if (uniqueQueue) {
            queueOptions.autoDelete = true;
        }
        const q = await channel.assertQueue(taskQueueName, queueOptions);
        await channel.bindQueue(q.queue, exchangeName, fullTaskName);
        const prefetch = (_d = (_c = options === null || options === void 0 ? void 0 : options.consumer) === null || _c === void 0 ? void 0 : _c.prefetch) !== null && _d !== void 0 ? _d : this.config.consumer.prefetch;
        await channel.prefetch(prefetch, false);
        const { consumerTag } = await channel.consume(q.queue, (message) => this._handleConsumeMessage(message, 'task', consumeMessageOptions, consumeFn), { consumerTag: options === null || options === void 0 ? void 0 : options.consumerTag });
        this.consumers.push({ type: 'task', key: taskName, consumerTag, consumeFn, options });
        return consumerTag;
    }
    async registerFailedMessageConsumer(consumeFn, options) {
        var _a, _b;
        const channel = await this._getChannel();
        const queueName = this.config.queues.failed;
        this.logger.trace({ queueName }, 'registerFailedMessageConsumer()');
        const q = await channel.assertQueue(queueName, options === null || options === void 0 ? void 0 : options.queue);
        const prefetch = (_b = (_a = options === null || options === void 0 ? void 0 : options.consumer) === null || _a === void 0 ? void 0 : _a.prefetch) !== null && _b !== void 0 ? _b : this.config.consumer.prefetch;
        await channel.prefetch(prefetch, false);
        const { consumerTag } = await channel.consume(q.queue, (message) => message && this._handleFailedMessage(message, { ...options, queueName }, consumeFn), { consumerTag: options === null || options === void 0 ? void 0 : options.consumerTag });
        this.consumers.push({ type: 'message', key: 'failed', consumerTag, consumeFn, options });
        return consumerTag;
    }
    async assertConnection() {
        await this._getConnection();
    }
    async _getChannel() {
        if (!this._channel) {
            if (!this._getChannelPromise) {
                this._getChannelPromise = (async () => {
                    try {
                        const connection = await this._getConnection();
                        this.logger.info({}, 'Opening channel');
                        this._channel = await connection.createChannel();
                        this._channel.on('error', err => {
                            this.logger.warn({ err }, 'RabbitMQ channel error');
                            delete this._channel;
                        });
                        this._channel.on('close', err => {
                            delete this._channel;
                            this._onChannelClosed(err);
                        });
                        await this._onChannelOpened(this._channel);
                    }
                    catch (err) {
                        this.logger.error({ err }, 'Error creating RabbitMQ channel');
                    }
                    finally {
                        delete this._getChannelPromise;
                    }
                })();
            }
            await this._getChannelPromise;
        }
        if (!this._channel) {
            throw new Error('Could not create channel to RabbitMQ');
        }
        return this._channel;
    }
    async _getConnection() {
        if (!this._conn) {
            if (this.isShuttingDown) {
                throw new Error('RabbitMQ module is shutting down');
            }
            if (!this._getConnectionPromise) {
                this._getConnectionPromise = (async () => {
                    try {
                        const connectionConfig = lodash_1.default.get(this.config, 'connection');
                        this.logger.info({ connectionConfig: lodash_1.default.omit(connectionConfig, ['password']) }, 'Opening connection to RabbitMQ');
                        this._conn = await amqplib_1.default.connect(CoinifyRabbit._generateConnectionUrl(connectionConfig), { clientProperties: { connection_name: lodash_1.default.get(this.config, 'service.name') } });
                        this._conn.on('error', err => {
                            this.logger.warn({ err }, 'RabbitMQ connection error');
                            delete this._conn;
                        });
                        this._conn.on('close', err => {
                            delete this._conn;
                            this.logger.info({ err }, 'Connection closed');
                        });
                        this.logger.info({}, 'Connection opened');
                    }
                    catch (err) {
                        this.logger.error({ err }, 'Error connecting to RabbitMQ');
                    }
                    finally {
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
    static _generateConnectionUrl(connectionConfig) {
        if (!lodash_1.default.includes(['amqp', 'amqps'], connectionConfig.protocol)) {
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
    async _onChannelOpened(channel) {
        this.logger.info({}, 'Channel opened');
        const prefetch = lodash_1.default.get(this.config, 'channel.prefetch');
        await channel.prefetch(prefetch, true);
        await this._recreateRegisteredConsumers();
    }
    async _recreateRegisteredConsumers() {
        const consumers = lodash_1.default.cloneDeep(this.consumers);
        this.consumers = [];
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
                    throw new Error(`Internal error: Unknown consumer type '${consumer.type}'`);
            }
        }
    }
    _onChannelClosed(err) {
        if (this.isShuttingDown) {
            this.logger.info({ err }, 'Channel closed');
            return;
        }
        this.logger.warn({ err }, 'Channel closed unexpectedsly.');
        if (!this.consumers.length) {
            return;
        }
        return this._connectWithBackoff();
    }
    _connectWithBackoff() {
        const fibonacciBackoff = backoff_1.default.fibonacci({
            initialDelay: 1000,
            maxDelay: 600000
        });
        fibonacciBackoff.on('ready', (number, delay) => {
            if (this.isShuttingDown) {
                return;
            }
            this.logger.info({}, `Connecting... (attempt ${number}, delay ${delay}ms)`);
            if (this._getChannelPromise) {
                return;
            }
            this._getChannel()
                .catch(err => {
                fibonacciBackoff.backoff();
                this.logger.warn({ err }, 'Error reconnecting to channel');
            });
        });
        fibonacciBackoff.backoff();
    }
    async shutdown(timeout) {
        if (this.isShuttingDown) {
            return;
        }
        this.isShuttingDown = true;
        const activeTaskConsumptions = this.activeMessageConsumptions.filter(c => 'taskName' in c);
        const activeEventConsumptions = this.activeMessageConsumptions.filter(c => 'eventName' in c);
        this.logger.info({
            registeredConsumers: JSON.stringify(this.consumers.map(c => lodash_1.default.pick(c, ['type', 'key', 'consumerTag']))),
            activeConsumption: {
                tasks: JSON.stringify(activeTaskConsumptions.map(c => lodash_1.default.pick(c, ['uuid', 'taskName']))),
                events: JSON.stringify(activeEventConsumptions.map(c => lodash_1.default.pick(c, ['uuid', 'eventName'])))
            },
            timeout
        }, 'Shutting down RabbitMQ');
        if (lodash_1.default.size(this.consumers)) {
            await this._cancelAllConsumers();
            await this._waitForConsumersToFinish(timeout);
        }
        if (lodash_1.default.size(this.activeMessageConsumptions)) {
            const channel = await this._getChannel();
            channel.nackAll();
        }
        if (this._channel) {
            this.logger.info({}, 'Closing channel');
            await this._channel.close();
            delete this._channel;
        }
        if (this._conn) {
            this.logger.info({}, 'Closing connection');
            await this._conn.close();
            delete this._conn;
        }
    }
    async _cancelAllConsumers() {
        const channel = await this._getChannel();
        this.logger.info({}, 'Cancelling all consumers');
        await Promise.all(this.consumers.map(c => channel.cancel(c.consumerTag)));
        this.consumers = [];
    }
    _waitForConsumersToFinish(timeout) {
        if (lodash_1.default.size(this.activeMessageConsumptions) === 0) {
            return;
        }
        return new Promise(resolve => {
            const resolveOnce = lodash_1.default.once(resolve);
            this.on('messageConsumed', () => {
                if (lodash_1.default.size(this.activeMessageConsumptions) === 0) {
                    resolveOnce(undefined);
                }
            });
            if (timeout && lodash_1.default.isInteger(timeout) && timeout > 0) {
                setTimeout(resolveOnce, timeout);
            }
        });
    }
    _getInstanceIdentifier() {
        return crypto_1.default.randomBytes(10).toString('hex');
    }
    _getConsumeEventQueueName(eventKey, serviceName, uniqueQueue = false) {
        let queueName = 'events.' + serviceName;
        if (uniqueQueue) {
            queueName += '.instance-' + this._getInstanceIdentifier();
        }
        queueName += '.' + eventKey;
        return queueName;
    }
    _getTaskConsumerQueueName(taskName, serviceName, uniqueQueue = false) {
        let queueName = 'tasks.' + serviceName;
        if (uniqueQueue) {
            queueName += '.instance-' + this._getInstanceIdentifier();
        }
        queueName += '.' + taskName;
        return queueName;
    }
    async _handleConsumeMessage(message, messageType, options, consumeFn) {
        if (message === null) {
            if (!options.onCancel) {
                return null;
            }
            return options.onCancel();
        }
        const channel = await this._getChannel();
        const msgObj = JSON.parse(message.content.toString());
        const context = msgObj.context;
        const name = messageType === 'event' ? msgObj.eventName : msgObj.taskName;
        this.logger.info({ [messageType]: msgObj }, `${messageType} ${name} ready to be consumed`);
        this.activeMessageConsumptions.push(msgObj);
        let consumeError = null;
        try {
            const startTime = Date.now();
            const consumeResult = await consumeFn(lodash_1.default.cloneDeep(context), lodash_1.default.cloneDeep(msgObj));
            const consumeTimeMillis = Date.now() - startTime;
            const consumeResultTruncated = lodash_1.default.truncate(JSON.stringify(consumeResult), { length: 4096 });
            this.logger.info({ [messageType]: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis }, `${messageType} ${name} consumed`);
        }
        catch (err) {
            consumeError = err;
        }
        lodash_1.default.pull(this.activeMessageConsumptions, msgObj);
        this.emit('messageConsumed', msgObj);
        try {
            channel.ack(message);
        }
        catch (err) {
            this.logger.error({ [messageType]: msgObj, err }, `Error ACK\'ing ${messageType}!`);
        }
        if (!consumeError) {
            return true;
        }
        return this._handleConsumeRejection(message, messageType, msgObj, consumeError, options);
    }
    async _handleConsumeRejection(message, messageType, messageObject, consumeError, options) {
        var _a;
        const allowedTypes = ['event', 'task'];
        if (!allowedTypes.includes(messageType)) {
            throw new Error(`Invalid type. Given: ${messageType}, allowed: [${allowedTypes.join(', ')}]`);
        }
        const retryResponse = CoinifyRabbit._decideConsumerRetry(messageObject.attempts, lodash_1.default.get(options, 'retry'));
        let { shouldRetry } = retryResponse;
        const { delaySeconds } = retryResponse;
        if (consumeError && consumeError.noRetry === true) {
            shouldRetry = false;
        }
        const messageName = 'eventName' in messageObject ? messageObject.eventName : messageObject.taskName;
        const onError = (_a = options === null || options === void 0 ? void 0 : options.onError) !== null && _a !== void 0 ? _a : (() => {
            const errMessage = `Error consuming ${messageType} ${messageName}: ${consumeError.message}. `
                + (shouldRetry ? `Will retry in ${delaySeconds} seconds` : 'No retry');
            if (shouldRetry) {
                this.logger.info({ err: consumeError, [messageType]: messageObject }, errMessage);
            }
            else {
                this.logger.error({ err: consumeError, [messageType]: messageObject }, errMessage);
            }
        });
        try {
            const onErrorArgs = {
                err: consumeError,
                context: messageObject.context,
                willRetry: shouldRetry,
                delaySeconds,
                event: 'eventName' in messageObject ? messageObject : undefined,
                task: 'taskName' in messageObject ? messageObject : undefined
            };
            await onError(onErrorArgs);
        }
        catch (err) {
            err.cause = consumeError;
            this.logger.error({ err, [messageType]: messageObject }, `${messageType} error handling function rejected!`);
        }
        const retryAmqpOptions = lodash_1.default.pick(options, ['exchange', 'queue']);
        const publishOptions = {};
        let republishExchangeName;
        if (shouldRetry) {
            const retryExchangeAndQueue = await this._assertRetryExchangeAndQueue(delaySeconds, retryAmqpOptions);
            republishExchangeName = retryExchangeAndQueue.retryExchangeName;
            publishOptions.BCC = retryExchangeAndQueue.retryQueueName;
        }
        else {
            republishExchangeName = await this._assertDeadLetterExchangeAndQueue(retryAmqpOptions);
        }
        messageObject = lodash_1.default.cloneDeep(messageObject);
        messageObject.attempts = (messageObject.attempts || 0) + 1;
        const updatedMessage = Buffer.from(JSON.stringify(messageObject));
        const channel = await this._getChannel();
        const routingKey = options.queueName;
        const publishResult = channel.publish(republishExchangeName, routingKey, updatedMessage, publishOptions);
        if (!publishResult) {
            const err = new Error(`channel.publish() to exchange '${republishExchangeName}' with routing key '${routingKey}'`
                + ` resolved to ${JSON.stringify(publishResult)}`);
            lodash_1.default.assign(err, { republishExchangeName, routingKey, updatedMessage });
            throw err;
        }
        const logContext = { [messageType]: messageObject, routingKey, shouldRetry, delaySeconds, publishOptions };
        if (shouldRetry) {
            this.logger.trace(logContext, `Scheduled ${messageType} for retry`);
        }
        else {
            this.logger.info(logContext, `${messageType} ${messageName} sent to dead-letter exchange without retry`);
        }
    }
    async _handleFailedMessage(message, options, consumeFn) {
        if (message === null) {
            if (!options.onCancel) {
                return null;
            }
            return options.onCancel();
        }
        const channel = await this._getChannel();
        const msgObj = JSON.parse(message.content.toString());
        this.logger.debug({ message: msgObj }, 'message from failed queue ready to be consumed');
        this.activeMessageConsumptions.push(msgObj);
        try {
            const startTime = Date.now();
            const consumeResult = await consumeFn(lodash_1.default.cloneDeep(message.fields.routingKey), lodash_1.default.cloneDeep(msgObj));
            const consumeTimeMillis = Date.now() - startTime;
            const consumeResultTruncated = lodash_1.default.truncate(JSON.stringify(consumeResult), { length: 4096 });
            channel.ack(message);
            this.logger.info({ message: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis }, 'message consumed');
        }
        catch (err) {
            this.logger.warn({ err, message: msgObj }, 'Error consuming message from failed queue');
            channel.nack(message);
        }
        lodash_1.default.pull(this.activeMessageConsumptions, msgObj);
        this.emit('messageConsumed', msgObj);
    }
    async enqueueMessage(queueName, messageObject, options) {
        const channel = await this._getChannel();
        const message = Buffer.from(JSON.stringify(messageObject));
        const exchangeName = '';
        const publishResult = channel.publish(exchangeName, queueName, message, options === null || options === void 0 ? void 0 : options.exchange);
        if (!publishResult) {
            throw new Error('channel.publish() resolved to ' + JSON.stringify(publishResult));
        }
        this.logger.info({ messageObject, exchangeName, options }, 'Enqueued message');
        return messageObject;
    }
    async _assertRetryExchangeAndQueue(delaySeconds, options) {
        const channel = await this._getChannel();
        const delayMs = Math.round(delaySeconds * 1000);
        const retryExchangeName = lodash_1.default.get(this.config, 'exchanges.retry');
        const retryQueueName = lodash_1.default.get(this.config, 'queues.retryPrefix') + '.' + delayMs + 'ms';
        const exchangeOptions = lodash_1.default.defaultsDeep({}, lodash_1.default.get(options, 'exchange', {}), { autoDelete: true });
        await channel.assertExchange(retryExchangeName, 'direct', exchangeOptions);
        const queueOptions = lodash_1.default.defaultsDeep({}, lodash_1.default.get(options, 'queue', {}), {
            deadLetterExchange: '',
            messageTtl: delayMs
        });
        const q = await channel.assertQueue(retryQueueName, queueOptions);
        await channel.bindQueue(q.queue, retryExchangeName, q.queue);
        return { retryExchangeName, retryQueueName };
    }
    async _assertDelayedTaskExchangeAndQueue(delayMillis, options) {
        const channel = await this._getChannel();
        const delayedExchangeName = this.config.exchanges.delayed;
        const delayedQueueName = this.config.queues.delayedTaskPrefix + '.' + delayMillis + 'ms';
        const exchangeOptions = { ...options === null || options === void 0 ? void 0 : options.exchange, autoDelete: true };
        const tasksExchangeName = this.config.exchanges.tasksTopic;
        await channel.assertExchange(delayedExchangeName, 'direct', exchangeOptions);
        const queueOptions = {
            ...options === null || options === void 0 ? void 0 : options.queue,
            expires: delayMillis + 10000,
            autoDelete: true,
            deadLetterExchange: tasksExchangeName,
            messageTtl: delayMillis
        };
        const q = await channel.assertQueue(delayedQueueName, queueOptions);
        await channel.bindQueue(q.queue, delayedExchangeName, q.queue);
        return { delayedExchangeName, delayedQueueName };
    }
    async _assertDeadLetterExchangeAndQueue(options) {
        const channel = await this._getChannel();
        const deadLetterExchangeName = this.config.exchanges.failed;
        const deadLetterQueueName = this.config.queues.failed;
        await channel.assertExchange(deadLetterExchangeName, 'fanout', options === null || options === void 0 ? void 0 : options.exchange);
        const q = await channel.assertQueue(deadLetterQueueName, options === null || options === void 0 ? void 0 : options.queue);
        await channel.bindQueue(q.queue, deadLetterExchangeName, '');
        return deadLetterExchangeName;
    }
    static _decideConsumerRetry(currentAttempt, options) {
        let delaySeconds = 0;
        if (!options) {
            return { shouldRetry: false, delaySeconds };
        }
        options = options || {};
        const maxAttempts = lodash_1.default.get(options, 'maxAttempts', 12);
        if (!lodash_1.default.isInteger(maxAttempts) || maxAttempts < -1) {
            throw new Error('Retry maxAttempts must be -1, 0, or a positive integer');
        }
        if (maxAttempts !== -1 && currentAttempt >= maxAttempts) {
            return { shouldRetry: false, delaySeconds };
        }
        delaySeconds = lodash_1.default.get(options, 'backoff.delay', 16);
        if (!lodash_1.default.isNumber(delaySeconds) || delaySeconds < 0) {
            throw new Error('Retry: backoff.delay must be a strictly positive number');
        }
        const backoffType = lodash_1.default.get(options, 'backoff.type', 'fixed');
        switch (backoffType) {
            case 'exponential': {
                const eBase = lodash_1.default.get(options, 'backoff.base', 2);
                delaySeconds = delaySeconds * eBase ** currentAttempt;
                break;
            }
            case 'fixed': {
                break;
            }
            default:
                throw new Error(`Retry: invalid backoff.type: '${backoffType}'`);
        }
        return { shouldRetry: true, delaySeconds };
    }
    static validateConsumerRetryOptions(options) {
        CoinifyRabbit._decideConsumerRetry(0, options);
    }
}
exports.default = CoinifyRabbit;
util_1.default.inherits(CoinifyRabbit, events_1.default);
module.exports = CoinifyRabbit;
