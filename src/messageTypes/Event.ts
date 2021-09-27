import amqplib from 'amqplib';
import { ServiceConfiguration } from '../CoinifyRabbitConfiguration';
import { RetryConfiguration } from '../types';

export default interface Event<Context = any> {
  eventName: string;
  context: Context;
  uuid: string;
  time: number;
  attempts: number;
}

export type EventConsumerFunction<Context = any, Result = any> = (context: Context, event: Event<Context>) => Promise<Result> | Result;

export interface EventConsumer<Context = any, Result = any> {
  type: 'event';
  consumerTag: string;
  key: string;
  consumeFn: EventConsumerFunction<Context, Result>;
  options?: RegisterEventConsumerOptions;
}

export interface OnEventErrorFunctionParams {
  err: Error;
  context: any;
  willRetry: boolean;
  delaySeconds: number;
  event: Event;
}

export interface RegisterEventConsumerOptions {
  consumerTag?: string;
  onCancel?: () => Promise<any>;
  onError?: (params: OnEventErrorFunctionParams) => Promise<void> | void;
  queue?: amqplib.Options.AssertQueue;
  exchange?: amqplib.Options.AssertExchange;
  uniqueQueue?: boolean;
  consumer?: {
    prefetch?: number;
  };
  retry?: RetryConfiguration;
  service?: ServiceConfiguration;
}

