"use strict";
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_CONFIGURATION = void 0;
exports.DEFAULT_CONFIGURATION = {
    connection: {
        host: process.env.RABBITMQ_HOST || 'localhost',
        port: parseInt((_a = process.env.RABBITMQ_PORT) !== null && _a !== void 0 ? _a : '') || 5672,
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
