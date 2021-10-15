import * as amqplib from 'amqplib';
import { ServiceConfiguration } from './CoinifyRabbitConfiguration';
export interface EnqueueMessageOptions {
    uuid?: string;
    time?: number;
    service?: ServiceConfiguration;
    exchange?: amqplib.Options.AssertExchange;
    usePublisherConfirm?: boolean;
}
export interface RetryExponentialBackoffConfiguration {
    type?: 'exponential';
    delay?: number;
    base?: number;
}
export interface RetryFixedBackoffConfiguration {
    type?: 'fixed';
    delay?: number;
}
export declare type RetryConfiguration = false | null | {
    backoff?: RetryExponentialBackoffConfiguration | RetryFixedBackoffConfiguration;
    maxAttempts?: number;
};
