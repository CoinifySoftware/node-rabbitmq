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
    publish: {
        persistentMessages: boolean;
    };
    consumer: {
        prefetch: number;
    };
    usePublisherConfirm: boolean;
    defaultLogLevel: 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';
}
export declare const DEFAULT_CONFIGURATION: CoinifyRabbitConfiguration;
