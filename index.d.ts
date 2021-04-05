import amqplib from 'amqplib';

interface Logger {
  debug(context?: any, message?: string): void;
  info(context?: any, message?: string): void;
  warn(context?: any, message?: string): void;
  error(context?: any, message?: string): void;
}

interface ServiceConfiguration {
  name?: string;
}

interface CoinifyRabbitConfiguration {
  service?: ServiceConfiguration;
  connection?: {
    host?: string;
    port?: number;
    protocol?: 'amqp' | 'amqps';
    vhost?: string;
    username?: string;
    password?: string;
  };
  channel?: {
    prefetch?: number;
  };
  exchanges?: {
    retry?: string;
    tasksTopic?: string;
    delayed?: string;
    failed?: string;
    eventsTopic?: string;
  };
  queues?: {
    retryPrefix?: string;
    delayedTaskPrefix?: string;
    failed?: string;
  };
  consumer?: {
    prefetch?: number;
  };
  defaultLogLevel?: string;
}

interface EnqueueMessageOptions {
  uuid?: string;
  time?: number | string;
  service?: ServiceConfiguration;
  exchange?: amqplib.Options.AssertExchange;
}

interface ConstructorOptions extends CoinifyRabbitConfiguration {
  logger?: Logger;
}

export interface Event {
  eventName: string;
  context: any;
  uuid: string;
  time: Date;
  attempts: number;
}

export interface Task {
  taskName: string;
  context: any;
  uuid: string;
  time: Date;
  attempts: number;
  origin: string;
  delayMillis: number;
}

interface RetryExponentialBackoffConfiguration {
  type?: 'exponential';
  delay?: number;
  base?: number
}

interface RetryFixedBackoffConfiguration {
  type?: 'fixed';
  delay?: number
}

interface RetryConfiguration {
  backoff?:  RetryExponentialBackoffConfiguration | RetryFixedBackoffConfiguration;
  maxAttempts?: number;
}

interface OnEventErrorFunctionParams {
  err: Error;
  context: any;
  willRetry: boolean;
  delaySeconds: number;
  event: Event;
}

interface RegisterEventConsumerOptions {
  consumerTag?: string;
  onCancel?: () => Promise<any>;
  onError?: (params: OnEventErrorFunctionParams) => Promise<any>;
  queue?: amqplib.Options.AssertQueue;
  exchange?: amqplib.Options.AssertExchange;
  uniqueQueue?: boolean;
  consumer?: {
    prefetch?: number;
  };
  retry?: RetryConfiguration;
  service?: ServiceConfiguration
}

interface OnTaskErrorFunctionParams {
  err: Error;
  context: any;
  willRetry: boolean;
  delaySeconds: number;
  task: Task;
}

interface RegisterTaskConsumerOptions {
  consumerTag?: string;
  onCancel?: () => Promise<any>;
  onError?: (params: OnTaskErrorFunctionParams) => Promise<any>;
  queue?: amqplib.Options.AssertQueue;
  exchange?: amqplib.Options.AssertExchange;
  uniqueQueue?: boolean;
  consumer?: {
    prefetch?: number;
  };
  retry?: RetryConfiguration;
  service?: ServiceConfiguration
}

interface RegisterFailedMessageConsumerOptions {
  consumerTag?: string;
  onCancel?: () => Promise<any>;
  onError?: (params: OnEventErrorFunctionParams | OnTaskErrorFunctionParams) => Promise<any>;
  queue?: amqplib.Options.AssertQueue;
  consumer?: {
    prefetch?: number;
  };
}

export default class CoinifyRabbit {
  constructor(options?: ConstructorOptions);

  emitEvent(eventName: string, context?: any, options?: EnqueueMessageOptions): Promise<any>;
  registerEventConsumer(eventKey: string, consumeFn: (context: any, event: Event) => Promise<any>, options?: RegisterEventConsumerOptions): Promise<any>;
  enqueueTask(fullTaskName: string, context?: any, options?: EnqueueMessageOptions): Promise<any>;
  registerTaskConsumer(taskName: string, consumeFn: (context: any, task: Task) => Promise<any>, options?: RegisterTaskConsumerOptions): Promise<any>;
  registerFailedMessageConsumer(consumeFn: (routingKey: any, message: Event | Task) => Promise<any>, options?: RegisterFailedMessageConsumerOptions): Promise<any>
  enqueueMessage(queueName: string, messageObject: Event | Task, options?: EnqueueMessageOptions): Promise<any>
  shutdown(timeout?: number): Promise<any>;
  assertConnection(): Promise<void>;
}
