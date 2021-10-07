import * as amqplib from 'amqplib';
import Event, { OnEventErrorFunctionParams } from './Event';
import Task, { OnTaskErrorFunctionParams } from './Task';

export type FailedMessage = Event | Task;
export type FailedMessageConsumerFunction<Result = any> = (queueName: string, task: FailedMessage) => Promise<Result> | Result;

export interface FailedMessageConsumer {
  type: 'message';
  consumerTag: string;
  key: string;
  consumeFn: FailedMessageConsumerFunction;
  options?: RegisterFailedMessageConsumerOptions;
}

export interface RegisterFailedMessageConsumerOptions {
  consumerTag?: string;
  onCancel?: () => Promise<any>;
  onError?: (params: OnEventErrorFunctionParams | OnTaskErrorFunctionParams) => Promise<void> | void;
  queue?: amqplib.Options.AssertQueue;
  consumer?: {
    prefetch?: number;
  };
}
