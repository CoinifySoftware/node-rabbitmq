import { expect } from 'chai';
import sinon from 'sinon';
import CoinifyRabbit from '../../../src/CoinifyRabbit';
import { getChannelPool } from '../../bootstrap.test';

describe('CoinifyRabbit', () => {

  describe('#_handleConsumeRejection', () => {

    let getPublisherChannelStub: sinon.SinonStub,
      channelPublishStub: sinon.SinonStub,
      _assertRetryExchangeAndQueueStub: sinon.SinonStub,
      _assertDeadLetterExchangeAndQueueStub: sinon.SinonStub,
      _decideConsumerRetryStub: sinon.SinonStub,
      _publisherChannel: any,

      rabbit: CoinifyRabbit;

    let message: any,
      task: any,
      options: any;

    const consumeError = new Error('The error');
    const retryExchangeName = 'the-exchange';
    const retryQueueName = 'the-retry-queue';

    beforeEach(() => {
      rabbit = new CoinifyRabbit({ defaultLogLevel: 'fatal' });

      message = {
        fields: {
          routingKey: 'the-routing-key'
        }
      };
      task = {
        attempts: 0,
        context: {
          theContext: true
        }
      };
      options = {
        retry: { retryOptions: true },
        exchange: { exchangeOptions: true },
        queue: { queueOptions: true },
        queueName: 'the-original-queue'
      };

      channelPublishStub = sinon.stub();
      channelPublishStub.resolves(true);
      _publisherChannel = { publish: channelPublishStub };
      getPublisherChannelStub = sinon.stub(getChannelPool(rabbit), 'getPublisherChannel');
      getPublisherChannelStub.resolves(_publisherChannel);
      _assertRetryExchangeAndQueueStub = sinon.stub(rabbit as any, '_assertRetryExchangeAndQueue');
      _assertRetryExchangeAndQueueStub.resolves({ retryExchangeName, retryQueueName });
      _assertDeadLetterExchangeAndQueueStub = sinon.stub(rabbit as any, '_assertDeadLetterExchangeAndQueue');
      _assertDeadLetterExchangeAndQueueStub.resolves(retryExchangeName);
      _decideConsumerRetryStub = sinon.stub(CoinifyRabbit, '_decideConsumerRetry');
      _decideConsumerRetryStub.returns({ shouldRetry: false, delaySeconds: 0 });
    });

    afterEach(() => {
      getPublisherChannelStub.restore();
      _assertRetryExchangeAndQueueStub.restore();
      _assertDeadLetterExchangeAndQueueStub.restore();
      _decideConsumerRetryStub.restore();
    });

    it('should re-publish to retry queue if _decideConsumerRetry() returns shouldRetry: true', async () => {
      const delaySeconds = 6660;

      _decideConsumerRetryStub.returns({ shouldRetry: true, delaySeconds });

      await (rabbit as any)._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(_decideConsumerRetryStub.calledOnce).to.equal(true);
      expect(_decideConsumerRetryStub.firstCall.args).to.deep.equal([ task.attempts, options.retry ]);

      expect(_assertRetryExchangeAndQueueStub.calledOnce).to.equal(true);
      expect(_assertRetryExchangeAndQueueStub.firstCall.args).to.deep.equal([ _publisherChannel, delaySeconds, { exchange: options.exchange, queue: options.queue } ]);

      expect(_assertDeadLetterExchangeAndQueueStub.notCalled).to.equal(true);

      expect(channelPublishStub.calledOnce).to.equal(true);
      expect(channelPublishStub.firstCall.args).to.have.lengthOf(4);
      expect(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
      expect(channelPublishStub.firstCall.args[1]).to.equal(options.queueName);
      expect(channelPublishStub.firstCall.args[2]).to.be.instanceof(Buffer);
      expect(channelPublishStub.firstCall.args[3]).to.deep.equal({ BCC: retryQueueName, persistent: true });
      const messageDecoded = JSON.parse(channelPublishStub.firstCall.args[2].toString());
      expect(messageDecoded).to.deep.equal({ ...task, attempts: task.attempts + 1 });
    });

    it('should re-publish to dead letter queue if _decideConsumerRetry() returns shouldRetry: false', async () => {
      await (rabbit as any)._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(_decideConsumerRetryStub.calledOnce).to.equal(true);
      expect(_decideConsumerRetryStub.firstCall.args).to.deep.equal([ task.attempts, options.retry ]);

      expect(_assertRetryExchangeAndQueueStub.notCalled).to.equal(true);

      expect(_assertDeadLetterExchangeAndQueueStub.calledOnce).to.equal(true);
      expect(_assertDeadLetterExchangeAndQueueStub.firstCall.args).to.deep.equal([ _publisherChannel, { exchange: options.exchange, queue: options.queue } ]);

      expect(channelPublishStub.calledOnce).to.equal(true);
      expect(channelPublishStub.firstCall.args).to.have.lengthOf(4);
      expect(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
      expect(channelPublishStub.firstCall.args[1]).to.equal(options.queueName);
      expect(channelPublishStub.firstCall.args[2]).to.be.instanceof(Buffer);
      expect(channelPublishStub.firstCall.args[3]).to.deep.equal({ persistent: true });
      const messageDecoded = JSON.parse(channelPublishStub.firstCall.args[2].toString());
      expect(messageDecoded).to.deep.equal({ ...task, attempts: task.attempts + 1 });
    });

    it('should re-publish to dead letter queue if consumeError contains noRetry: true property', async () => {
      const consumeError = new Error('Error that we should not retry on');
      (consumeError as any).noRetry = true;

      // _decideConsumerRetry returns shouldRetry: false, but we want to override it
      const delaySeconds = 6660;
      _decideConsumerRetryStub.returns({ shouldRetry: true, delaySeconds });

      await (rabbit as any)._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(_assertRetryExchangeAndQueueStub.notCalled).to.equal(true);

      expect(_assertDeadLetterExchangeAndQueueStub.calledOnce).to.equal(true);
      expect(_assertDeadLetterExchangeAndQueueStub.firstCall.args).to.deep.equal([ _publisherChannel, { exchange: options.exchange, queue: options.queue } ]);

      expect(channelPublishStub.calledOnce).to.equal(true);
      expect(channelPublishStub.firstCall.args).to.have.lengthOf(4);
      expect(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
      expect(channelPublishStub.firstCall.args[3]).to.deep.equal({ persistent: true });
    });

    it('should reject if type is neither task nor event', async () => {
      const errMsg = 'Invalid type. Given: invalid, allowed: [event, task]';

      await expect((rabbit as any)._handleConsumeRejection(message, 'invalid', task, consumeError, options))
        .to.eventually
        .be.rejectedWith(errMsg)
        .and.be.an.instanceOf(Error);
    });
  });
});
