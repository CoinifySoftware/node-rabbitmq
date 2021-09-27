import amqplib from 'amqplib';
import { expect } from 'chai';
import sinon from 'sinon';
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

    it('should set the channel prefetch limit', async () => {
      await (rabbit as any)._onChannelOpened(channel);

      expect(channelPrefetchStub.calledOnce).to.equal(true);
      expect(channelPrefetchStub.firstCall.args).to.deep.equal([ config.channel.prefetch, true ]);
    });

    it('should recreate registered consumers', async () => {
      await (rabbit as any)._onChannelOpened(channel);

      expect(_recreateRegisteredConsumersStub.calledOnce).to.equal(true);
      expect(_recreateRegisteredConsumersStub.firstCall.args).to.have.lengthOf(0);
    });

  });

});
