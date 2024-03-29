"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_clonedeep_1 = __importDefault(require("lodash.clonedeep"));
const lodash_defaultsdeep_1 = __importDefault(require("lodash.defaultsdeep"));
const lodash_truncate_1 = __importDefault(require("lodash.truncate"));
const amqplib = __importStar(require("amqplib"));
const backoff_1 = __importDefault(require("backoff"));
const console_log_level_1 = __importDefault(require("console-log-level"));
const crypto_1 = __importDefault(require("crypto"));
const uuid_1 = require("uuid");
const events_1 = require("events");
const CoinifyRabbitConfiguration_1 = require("./CoinifyRabbitConfiguration");
const ChannelPool_1 = __importStar(require("./ChannelPool"));
const util_1 = require("util");
class CoinifyRabbit extends events_1.EventEmitter {
    constructor(options) {
        super();
        this.consumers = [];
        this.activeMessageConsumptions = [];
        this.isShuttingDown = false;
        const { logger, ...config } = options !== null && options !== void 0 ? options : {};
        this.config = (0, lodash_defaultsdeep_1.default)({}, config, CoinifyRabbitConfiguration_1.DEFAULT_CONFIGURATION);
        this.logger = logger !== null && logger !== void 0 ? logger : (0, console_log_level_1.default)({ level: this.config.defaultLogLevel });
        this.channels = new ChannelPool_1.default(this.logger, () => this._getConnection(), (channel, type) => this._onChannelOpened(channel, type), (type, err) => Promise.resolve(this._onChannelClosed(type, err)));
        if (!this.config.service.name) {
            throw new Error('options.service.name must be set');
        }
    }
    async emitEvent(eventName, context, options) {
        var _a, _b, _c, _d;
        const serviceName = (_b = (_a = options === null || options === void 0 ? void 0 : options.service) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : this.config.service.name;
        const usePublisherConfirm = (_c = options === null || options === void 0 ? void 0 : options.usePublisherConfirm) !== null && _c !== void 0 ? _c : this.config.usePublisherConfirm;
        const fullEventName = serviceName ? serviceName + '.' + eventName : eventName;
        this.logger.trace({ fullEventName, context, options }, 'emitEvent()');
        const channel = await this.channels.getPublisherChannel(usePublisherConfirm);
        const exchangeName = this.config.exchanges.eventsTopic;
        await channel.assertExchange(exchangeName, 'topic', options === null || options === void 0 ? void 0 : options.exchange);
        const event = {
            eventName: fullEventName,
            context,
            uuid: (_d = options === null || options === void 0 ? void 0 : options.uuid) !== null && _d !== void 0 ? _d : (0, uuid_1.v4)(),
            time: (options === null || options === void 0 ? void 0 : options.time) ? new Date(options.time).getTime() : Date.now(),
            attempts: 0
        };
        const message = Buffer.from(JSON.stringify(event));
        await this.publishMessage(channel, exchangeName, fullEventName, message);
        this.logger.info({ event, exchangeName, options }, `Event ${event.eventName} emitted`);
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
        const channel = await this.channels.getConsumerChannel();
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
        var _a, _b, _c, _d, _e;
        const serviceName = (_b = (_a = options === null || options === void 0 ? void 0 : options.service) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : this.config.service.name;
        const usePublisherConfirm = (_c = options === null || options === void 0 ? void 0 : options.usePublisherConfirm) !== null && _c !== void 0 ? _c : this.config.usePublisherConfirm;
        const channel = await this.channels.getPublisherChannel(usePublisherConfirm);
        const delayMillis = (_d = options === null || options === void 0 ? void 0 : options.delayMillis) !== null && _d !== void 0 ? _d : 0;
        let exchangeName;
        const publishOptions = {};
        if (delayMillis > 0) {
            const delayedAmqpOptions = { exchange: options === null || options === void 0 ? void 0 : options.exchange };
            const { delayedExchangeName, delayedQueueName } = await this._assertDelayedTaskExchangeAndQueue(channel, delayMillis, delayedAmqpOptions);
            exchangeName = delayedExchangeName;
            publishOptions.BCC = delayedQueueName;
        }
        else {
            exchangeName = this.config.exchanges.tasksTopic;
            await channel.assertExchange(exchangeName, 'topic', options === null || options === void 0 ? void 0 : options.exchange);
        }
        this.logger.trace({ fullTaskName, context, exchangeName, options, publishOptions }, 'enqueueTask()');
        const task = {
            taskName: fullTaskName,
            context,
            uuid: (_e = options === null || options === void 0 ? void 0 : options.uuid) !== null && _e !== void 0 ? _e : (0, uuid_1.v4)(),
            time: (options === null || options === void 0 ? void 0 : options.time) ? new Date(options.time).getTime() : Date.now(),
            attempts: 0,
            origin: serviceName,
            delayMillis: delayMillis > 0 ? delayMillis : undefined
        };
        const message = Buffer.from(JSON.stringify(task));
        await this.publishMessage(channel, exchangeName, fullTaskName, message, publishOptions);
        this.logger.info({ task, exchangeName, options }, `Task ${task.taskName} enqueued`);
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
        const channel = await this.channels.getConsumerChannel();
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
        const channel = await this.channels.getConsumerChannel();
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
    async _getConnection() {
        if (!this._conn) {
            if (this.isShuttingDown) {
                throw new Error('RabbitMQ module is shutting down');
            }
            if (!this._getConnectionPromise) {
                this._getConnectionPromise = (async () => {
                    try {
                        const connectionConfig = this.config.connection;
                        this.logger.info({ connectionConfig: { ...connectionConfig, password: undefined } }, 'Opening connection to RabbitMQ');
                        this._conn = await amqplib.connect(CoinifyRabbit._generateConnectionUrl(connectionConfig), { clientProperties: { connection_name: this.config.service.name } });
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
        if (!['amqp', 'amqps'].includes(connectionConfig.protocol)) {
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
    async _onChannelOpened(channel, type) {
        this.logger.info({ type }, `RabbitMQ ${type} channel opened`);
        if (type === 'consumer') {
            const prefetch = this.config.channel.prefetch;
            await channel.prefetch(prefetch, true);
            await this._recreateRegisteredConsumers();
        }
    }
    async _recreateRegisteredConsumers() {
        const consumers = (0, lodash_clonedeep_1.default)(this.consumers);
        this.consumers = [];
        this.logger.info({
            consumers: consumers.map(c => ({ type: c.type, key: c.key, tag: c.consumerTag }))
        }, `Recreating ${consumers.length} registered consumers`);
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
    _onChannelClosed(type, err) {
        if (this.isShuttingDown) {
            this.logger.info({ err, type }, `RabbitMQ ${type} channel closed`);
            return;
        }
        this.logger.warn({ err, type }, `RabbitMQ ${type} channel closed unexpectedly: ${err ? err.message : 'No error given'}`);
        if (!this.consumers.length) {
            return;
        }
        return this._connectWithBackoff();
    }
    async publishMessage(channel, exchange, routingKey, content, options) {
        const publishOptions = {
            persistent: this.config.publish.persistentMessages,
            ...options
        };
        if ((0, ChannelPool_1.isConfirmChannel)(channel)) {
            await (0, util_1.promisify)(channel.publish).bind(channel)(exchange, routingKey, content, publishOptions);
        }
        else {
            const publishResult = channel.publish(exchange, routingKey, content, publishOptions);
            if (!publishResult) {
                this.logger.warn({
                    exchange, routingKey, content: content.toString(), publishOptions
                }, `channel.publish() to exchange '${exchange}' with routing key '${routingKey}' returned false`);
                throw new Error(`channel.publish() to exchange '${exchange}' with routing key '${routingKey}' returned false`);
            }
        }
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
            if (this._getConnectionPromise) {
                return;
            }
            this._getConnection()
                .catch(err => {
                fibonacciBackoff.backoff();
                this.logger.warn({ err }, 'Error reconnecting to RabbitMQ');
            });
        });
        fibonacciBackoff.backoff();
    }
    async shutdown(timeout) {
        if (this.isShuttingDown) {
            return;
        }
        this.isShuttingDown = true;
        const activeTaskConsumptions = this.activeMessageConsumptions.filter((m) => 'taskName' in m);
        const activeEventConsumptions = this.activeMessageConsumptions.filter((m) => 'eventName' in m);
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
        if (this.activeMessageConsumptions.length > 0) {
            const channel = await this.channels.getConsumerChannel();
            channel.nackAll();
        }
        await this.channels.close();
        if (this._conn) {
            this.logger.info({}, 'Closing connection');
            await this._conn.close();
            delete this._conn;
        }
    }
    async _cancelAllConsumers() {
        if (!this.channels.isChannelOpen('consumer')) {
            return;
        }
        const channel = await this.channels.getConsumerChannel();
        this.logger.info({}, 'Cancelling all consumers');
        await Promise.all(this.consumers.map(c => channel.cancel(c.consumerTag)));
        this.consumers = [];
    }
    _waitForConsumersToFinish(timeout) {
        if (this.activeMessageConsumptions.length === 0) {
            return;
        }
        return new Promise(resolve => {
            let resolved = false;
            const resolveOnce = (arg) => {
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
        const channel = await this.channels.getConsumerChannel();
        const msgObj = JSON.parse(message.content.toString());
        const context = msgObj.context;
        const name = messageType === 'event' ? msgObj.eventName : msgObj.taskName;
        this.logger.info({ [messageType]: msgObj }, `${messageType} ${name} ready to be consumed`);
        this.activeMessageConsumptions.push(msgObj);
        let consumeError = null;
        try {
            const startTime = Date.now();
            const consumeResult = await consumeFn((0, lodash_clonedeep_1.default)(context), (0, lodash_clonedeep_1.default)(msgObj));
            const consumeTimeMillis = Date.now() - startTime;
            const consumeResultTruncated = (0, lodash_truncate_1.default)(JSON.stringify(consumeResult), { length: 4096 });
            this.logger.info({ [messageType]: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis }, `${messageType} ${name} consumed`);
        }
        catch (err) {
            consumeError = err;
        }
        this.activeMessageConsumptions = this.activeMessageConsumptions.filter(msg => msg !== msgObj);
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
        const retryResponse = CoinifyRabbit._decideConsumerRetry(messageObject.attempts, options.retry);
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
        const retryAmqpOptions = { exchange: options.exchange, queue: options.queue };
        const publishOptions = {};
        const channel = await this.channels.getPublisherChannel(this.config.usePublisherConfirm);
        let republishExchangeName;
        if (shouldRetry) {
            const retryExchangeAndQueue = await this._assertRetryExchangeAndQueue(channel, delaySeconds, retryAmqpOptions);
            republishExchangeName = retryExchangeAndQueue.retryExchangeName;
            publishOptions.BCC = retryExchangeAndQueue.retryQueueName;
        }
        else {
            republishExchangeName = await this._assertDeadLetterExchangeAndQueue(channel, retryAmqpOptions);
        }
        messageObject = (0, lodash_clonedeep_1.default)(messageObject);
        messageObject.attempts = (messageObject.attempts || 0) + 1;
        const updatedMessage = Buffer.from(JSON.stringify(messageObject));
        const routingKey = options.queueName;
        await this.publishMessage(channel, republishExchangeName, routingKey, updatedMessage, publishOptions);
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
        const channel = await this.channels.getConsumerChannel();
        const msgObj = JSON.parse(message.content.toString());
        this.logger.debug({ message: msgObj }, 'message from failed queue ready to be consumed');
        this.activeMessageConsumptions.push(msgObj);
        try {
            const startTime = Date.now();
            const consumeResult = await consumeFn((0, lodash_clonedeep_1.default)(message.fields.routingKey), (0, lodash_clonedeep_1.default)(msgObj));
            const consumeTimeMillis = Date.now() - startTime;
            const consumeResultTruncated = (0, lodash_truncate_1.default)(JSON.stringify(consumeResult), { length: 4096 });
            channel.ack(message);
            this.logger.info({ message: msgObj, consumeResult: consumeResultTruncated, consumeTimeMillis }, 'message consumed');
        }
        catch (err) {
            this.logger.warn({ err, message: msgObj }, 'Error consuming message from failed queue');
            channel.nack(message);
        }
        this.activeMessageConsumptions = this.activeMessageConsumptions.filter(msg => msg !== msgObj);
        this.emit('messageConsumed', msgObj);
    }
    async enqueueMessage(queueName, messageObject, options) {
        var _a;
        const usePublisherConfirm = (_a = options === null || options === void 0 ? void 0 : options.usePublisherConfirm) !== null && _a !== void 0 ? _a : this.config.usePublisherConfirm;
        const channel = await this.channels.getPublisherChannel(usePublisherConfirm);
        const message = Buffer.from(JSON.stringify(messageObject));
        const exchangeName = '';
        await this.publishMessage(channel, exchangeName, queueName, message, options === null || options === void 0 ? void 0 : options.exchange);
        this.logger.info({ messageObject, exchangeName, options }, 'Enqueued message');
        return messageObject;
    }
    async _assertRetryExchangeAndQueue(channel, delaySeconds, options) {
        const delayMs = Math.round(delaySeconds * 1000);
        const retryExchangeName = this.config.exchanges.retry;
        const retryQueueName = this.config.queues.retryPrefix + '.' + delayMs + 'ms';
        const exchangeOptions = (0, lodash_defaultsdeep_1.default)({}, options === null || options === void 0 ? void 0 : options.exchange, { autoDelete: true });
        await channel.assertExchange(retryExchangeName, 'direct', exchangeOptions);
        const queueOptions = (0, lodash_defaultsdeep_1.default)({}, options === null || options === void 0 ? void 0 : options.queue, {
            deadLetterExchange: '',
            messageTtl: delayMs
        });
        const q = await channel.assertQueue(retryQueueName, queueOptions);
        await channel.bindQueue(q.queue, retryExchangeName, q.queue);
        return { retryExchangeName, retryQueueName };
    }
    async _assertDelayedTaskExchangeAndQueue(channel, delayMillis, options) {
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
    async _assertDeadLetterExchangeAndQueue(channel, options) {
        const deadLetterExchangeName = this.config.exchanges.failed;
        const deadLetterQueueName = this.config.queues.failed;
        await channel.assertExchange(deadLetterExchangeName, 'fanout', options === null || options === void 0 ? void 0 : options.exchange);
        const q = await channel.assertQueue(deadLetterQueueName, options === null || options === void 0 ? void 0 : options.queue);
        await channel.bindQueue(q.queue, deadLetterExchangeName, '');
        return deadLetterExchangeName;
    }
    static _decideConsumerRetry(currentAttempt, options) {
        var _a, _b, _c, _d, _e, _f, _g;
        let delaySeconds = 0;
        if (!options) {
            return { shouldRetry: false, delaySeconds };
        }
        const maxAttempts = (_a = options.maxAttempts) !== null && _a !== void 0 ? _a : 12;
        if (!Number.isInteger(maxAttempts) || maxAttempts < -1) {
            throw new Error('Retry maxAttempts must be -1, 0, or a positive integer');
        }
        if (maxAttempts !== -1 && currentAttempt >= maxAttempts) {
            return { shouldRetry: false, delaySeconds };
        }
        delaySeconds = (_c = (_b = options.backoff) === null || _b === void 0 ? void 0 : _b.delay) !== null && _c !== void 0 ? _c : 16;
        if (!Number.isFinite(delaySeconds) || delaySeconds < 0) {
            throw new Error('Retry: backoff.delay must be a strictly positive number');
        }
        switch ((_d = options.backoff) === null || _d === void 0 ? void 0 : _d.type) {
            case 'exponential': {
                const eBase = (_f = (_e = options.backoff) === null || _e === void 0 ? void 0 : _e.base) !== null && _f !== void 0 ? _f : 2;
                delaySeconds = delaySeconds * eBase ** currentAttempt;
                break;
            }
            case undefined:
            case 'fixed':
                break;
            default:
                throw new Error(`Retry: invalid backoff.type: '${(_g = options.backoff) === null || _g === void 0 ? void 0 : _g.type}'`);
        }
        return { shouldRetry: true, delaySeconds };
    }
    static validateConsumerRetryOptions(options) {
        CoinifyRabbit._decideConsumerRetry(0, options);
    }
}
exports.default = CoinifyRabbit;
