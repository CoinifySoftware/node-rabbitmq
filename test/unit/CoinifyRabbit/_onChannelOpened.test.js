'use strict';

const sinon = require('sinon');

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

describe('CoinifyRabbit', () => {

  describe('#_onChannelOpened', () => {

    let _recreateRegisteredConsumersStub,
      channelPrefetchStub,
      channel,
      rabbit;

    const config = {channel: {prefetch: 123}};

    beforeEach(() => {
      rabbit = new CoinifyRabbit(config);

      _recreateRegisteredConsumersStub = sinon.stub(rabbit, '_recreateRegisteredConsumers');
      _recreateRegisteredConsumersStub.resolves();

      channelPrefetchStub = sinon.stub();
      channelPrefetchStub.resolves();

      channel = {prefetch: channelPrefetchStub};
    });

    afterEach(() => {
      _recreateRegisteredConsumersStub.restore();
    });

    it('should set the channel prefetch limit', async () => {
      await rabbit._onChannelOpened(channel);

      expect(channelPrefetchStub.calledOnce).to.equal(true);
      expect(channelPrefetchStub.firstCall.args).to.deep.equal([config.channel.prefetch, true]);
    });

    it('should recreate registered consumers', async () => {
      await rabbit._onChannelOpened(channel);

      expect(_recreateRegisteredConsumersStub.calledOnce).to.equal(true);
      expect(_recreateRegisteredConsumersStub.firstCall.args).to.have.lengthOf(0);
    });

  });

});