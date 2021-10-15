export interface ServiceConfiguration {
  name: string;
}

export interface CoinifyRabbitConnectionConfiguration {
  host: string;
  port?: number;
  protocol: 'amqp' | 'amqps';
  vhost?: string;
  username?: string;
  password?: string;
}

export default interface CoinifyRabbitConfiguration {
  service: ServiceConfiguration;
  connection: CoinifyRabbitConnectionConfiguration;
  channel: {
    prefetch: number;
  };
  exchanges: {
    retry: string;
    tasksTopic: string;
    delayed: string;
    failed: string;
    eventsTopic: string;
  };
  queues: {
    retryPrefix: string;
    delayedTaskPrefix: string;
    failed: string;
  };
  consumer: {
    prefetch: number;
  };
  usePublisherConfirm: boolean;
  defaultLogLevel: 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';
}

export const DEFAULT_CONFIGURATION: CoinifyRabbitConfiguration = {
  connection: {
    host: process.env.RABBITMQ_HOST || 'localhost',
    port: parseInt(process.env.RABBITMQ_PORT ?? '') || 5672,
    protocol: 'amqp',
    vhost: process.env.RABBITMQ_VHOST || '',
    username: process.env.RABBITMQ_USERNAME || 'guest',
    password: process.env.RABBITMQ_PASSWORD || 'guest'
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
    delayed: '_delayed',
    failed: '_failed',
    eventsTopic: 'events.topic'
  },
  queues: {
    retryPrefix: '_retry',
    delayedTaskPrefix: '_delay.tasks',
    failed: '_failed'
  },
  consumer: {
    prefetch: 2
  },
  usePublisherConfirm: true,
  defaultLogLevel: 'error'
};
