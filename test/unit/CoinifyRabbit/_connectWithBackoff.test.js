'use strict';

const sinon = require('sinon'),
  backoff = require('backoff'),
  EventEmitter = require('events');

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

describe('CoinifyRabbit', () => {

  describe('#_connectWithBackoff', () => {

    let _getChannelStub,
      backoffFibonacciStub,
      rabbit;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      _getChannelStub = sinon.stub(rabbit, '_getChannel');
      _getChannelStub.resolves();

      backoffFibonacciStub = sinon.stub(backoff, 'fibonacci');
      backoffFibonacciStub.throws(new Error('Should not be called'));
    });

    afterEach(() => {
      _getChannelStub.restore();
      backoffFibonacciStub.restore();
    });

    it('should call _getChannel() with backoff', async () => {
      const backoffAttempts = 5;

      const rejectError = new Error('Error');
      // _getChannel rejects first 4 times, resolves the 5th time
      _getChannelStub.rejects(rejectError);
      _getChannelStub.onCall(backoffAttempts - 1).resolves();

      const fibonacci = new EventEmitter();
      fibonacci.backoff = sinon.stub();
      fibonacci.backoff.returns();

      backoffFibonacciStub.returns(fibonacci);

      await rabbit._connectWithBackoff();

      // Backoff initiated
      expect(backoffFibonacciStub.calledOnce).to.equal(true);
      expect(fibonacci.backoff.calledOnce).to.equal(true);
      expect(_getChannelStub.notCalled).to.equal(true);

      // Now, fake some backoff calls
      for (let i = 0; i < backoffAttempts; i++) {
        fibonacci.emit('ready', i, 0);
      }

      // wait a moment
      await new Promise(resolve => process.nextTick(resolve));

      expect(_getChannelStub.callCount).to.equal(backoffAttempts);
      expect(fibonacci.backoff.callCount).to.equal(backoffAttempts);
    });

  });

});