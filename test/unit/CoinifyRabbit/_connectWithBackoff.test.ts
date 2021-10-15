import backoff from 'backoff';
import { expect } from 'chai';
import EventEmitter from 'events';
import sinon from 'sinon';
import CoinifyRabbit from '../../../src/CoinifyRabbit';

describe('CoinifyRabbit', () => {

  describe('#_connectWithBackoff', () => {

    let _getConnectionStub: sinon.SinonStub,
      backoffFibonacciStub: sinon.SinonStub,
      rabbit: CoinifyRabbit;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      _getConnectionStub = sinon.stub(rabbit, '_getConnection');
      _getConnectionStub.resolves();

      backoffFibonacciStub = sinon.stub(backoff, 'fibonacci');
      backoffFibonacciStub.throws(new Error('Should not be called'));
    });

    afterEach(() => {
      _getConnectionStub.restore();
      backoffFibonacciStub.restore();
    });

    it('should call _getConnection() with backoff', async () => {
      const backoffAttempts = 5;

      const rejectError = new Error('Error');
      // _getConnection rejects first 4 times, resolves the 5th time
      _getConnectionStub.rejects(rejectError);
      _getConnectionStub.onCall(backoffAttempts - 1).resolves();

      // Sorry about that ugly type
      const fibonacci: EventEmitter | ( any & { backoff: any } ) = new EventEmitter();
      fibonacci.backoff = sinon.stub();
      fibonacci.backoff.returns();

      backoffFibonacciStub.returns(fibonacci);

      await (rabbit as any)._connectWithBackoff();

      // Backoff initiated
      expect(backoffFibonacciStub.calledOnce).to.equal(true);
      expect(fibonacci.backoff.calledOnce).to.equal(true);
      expect(_getConnectionStub.notCalled).to.equal(true);

      // Now, fake some backoff calls
      for (let i = 0; i < backoffAttempts; i++) {
        fibonacci.emit('ready', i, 0);
      }

      // wait a moment
      await new Promise(resolve => process.nextTick(resolve));

      expect(_getConnectionStub.callCount).to.equal(backoffAttempts);
      expect(fibonacci.backoff.callCount).to.equal(backoffAttempts);
    });

  });

});
