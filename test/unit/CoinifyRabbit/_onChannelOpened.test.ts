import amqplib from 'amqplib';
import { expect } from 'chai';
import sinon from 'sinon';
import { ChannelType } from '../../../src/ChannelPool';
import CoinifyRabbit from '../../../src/CoinifyRabbit';

describe('CoinifyRabbit', () => {

  describe('#_onChannelOpened', () => {

    let _recreateRegisteredConsumersStub: sinon.SinonStub,
      channelPrefetchStub: sinon.SinonStub,
      channel: amqplib.Channel,
      rabbit: CoinifyRabbit;

    const config = { channel: { prefetch: 123 } };

    beforeEach(() => {
      rabbit = new CoinifyRabbit(config);

      _recreateRegisteredConsumersStub = sinon.stub(rabbit as any, '_recreateRegisteredConsumers');
      _recreateRegisteredConsumersStub.resolves();

      channelPrefetchStub = sinon.stub();
      channelPrefetchStub.resolves();

      channel = { prefetch: channelPrefetchStub } as any as amqplib.Channel;
    });

    afterEach(() => {
      _recreateRegisteredConsumersStub.restore();
    });

    const publisherChannelTypes: ChannelType[] = [ 'publisher', 'publisher-confirm' ];
    publisherChannelTypes.forEach(type => {
      it(`should neither set channel prefetch limit nor recreate consumers for ${type} channel`, async () => {
        await (rabbit as any)._onChannelOpened(channel, type);

        expect(channelPrefetchStub.callCount).to.equal(0);
        expect(_recreateRegisteredConsumersStub.callCount).to.equal(0);
      });
    });

    it('should set the channel prefetch limit for consumer channel', async () => {
      await (rabbit as any)._onChannelOpened(channel, 'consumer');

      expect(channelPrefetchStub.callCount).to.equal(1);
      expect(channelPrefetchStub.firstCall.args).to.deep.equal([ config.channel.prefetch, true ]);
    });

    it('should recreate registered consumers for consumer channel', async () => {
      await (rabbit as any)._onChannelOpened(channel, 'consumer');

      expect(_recreateRegisteredConsumersStub.callCount).to.equal(1);
      expect(_recreateRegisteredConsumersStub.firstCall.args).to.have.lengthOf(0);
    });

  });

});
