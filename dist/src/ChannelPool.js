"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isConfirmChannel = void 0;
class ChannelPool {
    constructor(logger, getConnection, onChannelOpened, onChannelClosed) {
        this.logger = logger;
        this.getConnection = getConnection;
        this.onChannelOpened = onChannelOpened;
        this.onChannelClosed = onChannelClosed;
        this.channels = {};
        this.getChannelPromise = {};
    }
    getConsumerChannel() {
        return this.getChannel('consumer');
    }
    getPublisherChannel(usePublisherConfirm) {
        return this.getChannel(usePublisherConfirm ? 'publisher-confirm' : 'publisher');
    }
    async close() {
        await Promise.all(Object.keys(this.channels).map(type => this.closeChannel(type)));
    }
    isChannelOpen(type) {
        return this.channels[type] !== undefined;
    }
    async getChannel(type) {
        if (!this.channels[type]) {
            if (!this.getChannelPromise[type]) {
                this.getChannelPromise[type] = (async () => {
                    try {
                        const connection = await this.getConnection();
                        this.logger.debug({ type }, `Opening RabbitMQ ${type} channel`);
                        const channel = type === 'publisher-confirm'
                            ? await connection.createConfirmChannel()
                            : await connection.createChannel();
                        channel.on('error', err => {
                            this.logger.warn({ err, type }, `RabbitMQ ${type} channel error: ${err.message}`);
                            this.channels[type] = undefined;
                        });
                        channel.on('close', async (err) => {
                            this.channels[type] = undefined;
                            try {
                                await this.onChannelClosed(type, err);
                            }
                            catch (err) {
                                this.logger.warn({ err, type }, 'RabbitMQ Channel onChannelClose rejected!');
                            }
                        });
                        this.channels[type] = channel;
                        await this.onChannelOpened(channel, type);
                    }
                    catch (err) {
                        const errorMessage = err instanceof Error ? err.message : JSON.stringify(err);
                        this.logger.error({ err, type }, `Error creating RabbitMQ ${type} channel: ${errorMessage}`);
                        throw err;
                    }
                    finally {
                        this.getChannelPromise[type] = undefined;
                    }
                })();
            }
            await this.getChannelPromise[type];
        }
        const channel = this.channels[type];
        if (!channel) {
            throw new Error(`Could not create ${type} channel to RabbitMQ`);
        }
        return channel;
    }
    async closeChannel(type) {
        const channel = this.channels[type];
        if (!channel) {
            return;
        }
        this.logger.debug({}, `Clossing RabbitMQ ${type} channel`);
        this.channels[type] = undefined;
        await channel.close();
    }
}
exports.default = ChannelPool;
function isConfirmChannel(channel) {
    return 'waitForConfirms' in channel;
}
exports.isConfirmChannel = isConfirmChannel;
