/// <reference types="node" />
import * as amqplib from 'amqplib';
import { EventEmitter } from 'events';
import CoinifyRabbitConfiguration, { CoinifyRabbitConnectionConfiguration } from './CoinifyRabbitConfiguration';
import Logger from './interfaces/Logger';
import DeepPartial from './DeepPartial';
import { EnqueueMessageOptions, RetryConfiguration } from './types';
import Event, { EventConsumerFunction, RegisterEventConsumerOptions } from './messageTypes/Event';
import Task, { EnqueueTaskOptions, RegisterTaskConsumerOptions, TaskConsumerFunction } from './messageTypes/Task';
import { FailedMessageConsumerFunction, RegisterFailedMessageConsumerOptions } from './messageTypes/FailedMessage';
export interface CoinifyRabbitConstructorOptions extends DeepPartial<CoinifyRabbitConfiguration> {
    logger?: Logger;
}
export default class CoinifyRabbit extends EventEmitter {
    private config;
    private logger;
    private consumers;
    private activeMessageConsumptions;
    private isShuttingDown;
    constructor(options?: CoinifyRabbitConstructorOptions);
    emitEvent(eventName: string, context: unknown, options?: EnqueueMessageOptions): Promise<Event>;
    registerEventConsumer<Context = any>(eventKey: string, consumeFn: EventConsumerFunction<Context>, options?: RegisterEventConsumerOptions): Promise<string>;
    enqueueTask(fullTaskName: string, context: unknown, options?: EnqueueTaskOptions): Promise<Task<any>>;
    registerTaskConsumer<Context = any>(taskName: string, consumeFn: TaskConsumerFunction<Context>, options?: RegisterTaskConsumerOptions): Promise<string>;
    registerFailedMessageConsumer(consumeFn: FailedMessageConsumerFunction, options?: RegisterFailedMessageConsumerOptions): Promise<string>;
    assertConnection(): Promise<void>;
    private _channel?;
    private _getChannelPromise?;
    _getChannel(): Promise<amqplib.Channel>;
    private _conn?;
    private _getConnectionPromise?;
    _getConnection(): Promise<amqplib.Connection>;
    static _generateConnectionUrl(connectionConfig: CoinifyRabbitConnectionConfiguration): string;
    private _onChannelOpened;
    private _recreateRegisteredConsumers;
    private _onChannelClosed;
    private _connectWithBackoff;
    shutdown(timeout?: number): Promise<void>;
    private _cancelAllConsumers;
    private _waitForConsumersToFinish;
    _getInstanceIdentifier(): string;
    _getConsumeEventQueueName(eventKey: string, serviceName: string, uniqueQueue?: boolean): string;
    _getTaskConsumerQueueName(taskName: string, serviceName: string, uniqueQueue?: boolean): string;
    private _handleConsumeMessage;
    private _handleConsumeRejection;
    private _handleFailedMessage;
    enqueueMessage(queueName: string, messageObject: Event | Task, options?: {
        exchange?: amqplib.Options.Publish;
    }): Promise<Event<any> | Task<any>>;
    private _assertRetryExchangeAndQueue;
    private _assertDelayedTaskExchangeAndQueue;
    private _assertDeadLetterExchangeAndQueue;
    static _decideConsumerRetry(currentAttempt: number, options?: RetryConfiguration): {
        shouldRetry: boolean;
        delaySeconds: number;
    };
    static validateConsumerRetryOptions(options?: RetryConfiguration): void;
}
