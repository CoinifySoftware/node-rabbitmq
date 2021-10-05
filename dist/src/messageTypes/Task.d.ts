import * as amqplib from 'amqplib';
import { ServiceConfiguration } from '../CoinifyRabbitConfiguration';
import { RetryConfiguration } from '../types';
export default interface Task<Context = any> {
    taskName: string;
    context: Context;
    uuid: string;
    time: number;
    attempts: number;
    origin: string;
    delayMillis?: number;
}
export declare type TaskConsumerFunction<Context = any, Result = any> = (context: Context, task: Task<Context>) => Promise<Result> | Result;
export interface TaskConsumer<Context = any, Result = any> {
    type: 'task';
    consumerTag: string;
    key: string;
    consumeFn: TaskConsumerFunction<Context, Result>;
    options?: RegisterTaskConsumerOptions;
}
export interface OnTaskErrorFunctionParams {
    err: Error;
    context: any;
    willRetry: boolean;
    delaySeconds: number;
    task: Task;
}
export interface RegisterTaskConsumerOptions {
    consumerTag?: string;
    onCancel?: () => Promise<any>;
    onError?: (params: OnTaskErrorFunctionParams) => Promise<void> | void;
    queue?: amqplib.Options.AssertQueue;
    exchange?: amqplib.Options.AssertExchange;
    uniqueQueue?: boolean;
    consumer?: {
        prefetch?: number;
    };
    retry?: RetryConfiguration;
    service?: ServiceConfiguration;
}
