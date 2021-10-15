import * as amqplib from 'amqplib';
import Logger from './interfaces/Logger';
export declare type ChannelType = 'consumer' | 'publisher' | 'publisher-confirm';
export default class ChannelPool {
    private logger;
    private getConnection;
    private onChannelOpened;
    private onChannelClosed;
    private channels;
    private getChannelPromise;
    constructor(logger: Logger, getConnection: () => Promise<amqplib.Connection>, onChannelOpened: (channel: amqplib.Channel, type: ChannelType) => Promise<void>, onChannelClosed: (type: ChannelType, err?: Error) => Promise<void>);
    getConsumerChannel(): Promise<amqplib.Channel>;
    getPublisherChannel(usePublisherConfirm: boolean): Promise<amqplib.Channel>;
    close(): Promise<void>;
    isChannelOpen(type: ChannelType): boolean;
    private getChannel;
    private closeChannel;
}
export declare function isConfirmChannel(channel: amqplib.Channel): channel is amqplib.ConfirmChannel;
