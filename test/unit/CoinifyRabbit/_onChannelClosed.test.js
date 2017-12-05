'use strict';

const sinon = require('sinon');

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

describe('CoinifyRabbit', () => {

  describe('#_onChannelClosed', () => {

    let _connectWithBackoffStub,
      rabbit;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      _connectWithBackoffStub = sinon.stub(rabbit, '_connectWithBackoff');
      _connectWithBackoffStub.resolves();
    });

    afterEach(() => {
      _connectWithBackoffStub.restore();
    });

    it('should do nothing if shutdown was initiated', async () => {
      // Expected closing of channel
      rabbit._isShuttingDown = true;

      await rabbit._onChannelClosed();

      expect(_connectWithBackoffStub.notCalled).to.equal(true);
    });

    it('should do nothing on unexpected closing if there are no registered consumers', async () => {
      // Unexpected closing of channel
      rabbit._isShuttingDown = false;

      await rabbit._onChannelClosed();

      expect(_connectWithBackoffStub.notCalled).to.equal(true);
    });

    it('should call _connectWithBackoff() on unexpected closing if there are registered consumers', async() => {
      // Unexpected closing of channel
      rabbit._isShuttingDown = false;
      // Function only checks if _registeredConsumers is non-empty. Adding an empty object will do just fine.
      rabbit._registeredConsumers.push({});

      await rabbit._onChannelClosed();

      expect(_connectWithBackoffStub.calledOnce).to.equal(true);

    });

  });

});