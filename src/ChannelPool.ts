import * as amqplib from 'amqplib';
import Logger from './interfaces/Logger';

export type ChannelType = 'consumer' | 'publisher' | 'publisher-confirm';

export default class ChannelPool {

  private channels: { [type in ChannelType]?: amqplib.Channel } = {};
  private getChannelPromise: { [type in ChannelType]?: Promise<void> } = {};

  constructor(
    private logger: Logger,
    private getConnection: () => Promise<amqplib.ChannelModel>,
    private onChannelOpened: (channel: amqplib.Channel, type: ChannelType) => Promise<void>,
    private onChannelClosed: (type: ChannelType, err?: Error) => Promise<void>
  ) { }

  getConsumerChannel(): Promise<amqplib.Channel> {
    return this.getChannel('consumer');
  }
  getPublisherChannel(usePublisherConfirm: boolean): Promise<amqplib.Channel> {
    return this.getChannel(usePublisherConfirm ? 'publisher-confirm' : 'publisher');
  }

  async close(): Promise<void> {
    await Promise.all(Object.keys(this.channels).map(type => this.closeChannel(type as ChannelType)));
  }

  isChannelOpen(type: ChannelType): boolean {
    return this.channels[type] !== undefined;
  }

  private async getChannel(type: ChannelType): Promise<amqplib.Channel> {
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
              /*
                 A channel will emit 'error' if the server closes the channel for any reason. Such reasons include
                 * an operation failed due to a failed precondition (usually something named in an argument not existing)
                 * an human closed the channel with an admin tool

                 A channel will not emit 'error' if its connection closes with an error.
                 - https://www.squaremobius.net/amqp.node/channel_api.html#channel_events
              */
              this.logger.warn({ err, type }, `RabbitMQ ${type} channel error: ${err.message}`);
              // Discard current channel on error so a new one will be created next time
              this.channels[type] = undefined;
            });

            channel.on('close', async (err: Error | undefined) => {
              this.channels[type] = undefined;
              try {
                await this.onChannelClosed(type, err);
              } catch (err) {
                this.logger.warn({ err, type }, 'RabbitMQ Channel onChannelClose rejected!');
              }
            });

            this.channels[type] = channel;
            await this.onChannelOpened(channel, type);
          } catch (err) {
            const errorMessage = err instanceof Error ? err.message : JSON.stringify(err);
            this.logger.error({ err, type }, `Error creating RabbitMQ ${type} channel: ${errorMessage}`);
            throw err;
          } finally {
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

  private async closeChannel(type: ChannelType): Promise<void> {
    const channel = this.channels[type];
    if (!channel) {
      return;
    }

    this.logger.debug({}, `Clossing RabbitMQ ${type} channel`);
    this.channels[type] = undefined;
    await channel.close();
  }
}

export function isConfirmChannel(channel: amqplib.Channel): channel is amqplib.ConfirmChannel {
  return 'waitForConfirms' in channel;
}
