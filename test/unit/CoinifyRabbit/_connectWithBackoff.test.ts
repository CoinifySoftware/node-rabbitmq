import backoff from 'backoff';
import { expect } from 'chai';
import EventEmitter from 'events';
import sinon from 'sinon';
import CoinifyRabbit from '../../../src/CoinifyRabbit';

describe('CoinifyRabbit', () => {

  describe('#_connectWithBackoff', () => {

    let _getChannelStub: sinon.SinonStub,
      backoffFibonacciStub: sinon.SinonStub,
      rabbit: CoinifyRabbit;

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

      // Sorry about that ugly type
      const fibonacci: EventEmitter | ( any & { backoff: any } ) = new EventEmitter();
      fibonacci.backoff = sinon.stub();
      fibonacci.backoff.returns();

      backoffFibonacciStub.returns(fibonacci);

      await (rabbit as any)._connectWithBackoff();

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
