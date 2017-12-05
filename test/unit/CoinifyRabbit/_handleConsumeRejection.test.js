'use strict';

const CoinifyRabbit = require('../../../lib/CoinifyRabbit'),
  sinon = require('sinon'),
  _ = require('lodash');

describe('CoinifyRabbit', () => {

  describe('#_handleConsumeRejection', () => {

    let _getChannelStub,
      channelPublishStub,
      _assertRetryExchangeAndQueueStub,
      _assertDeadLetterExchangeAndQueueStub,
      _decideConsumerRetryStub,

      rabbit;

    let message,
      task,
      options;

    const consumeError = new Error('The error');
    const retryExchangeName = 'the-exchange';
    const retryQueueName = 'the-retry-queue';

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

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
        retry: {retryOptions: true},
        exchange: {exchangeOptions: true},
        queue: {queueOptions: true},
        queueName: 'the-original-queue'
      };

      channelPublishStub = sinon.stub();
      channelPublishStub.resolves(true);
      _getChannelStub = sinon.stub(rabbit, '_getChannel');
      _getChannelStub.resolves({publish: channelPublishStub});
      _assertRetryExchangeAndQueueStub = sinon.stub(rabbit, '_assertRetryExchangeAndQueue');
      _assertRetryExchangeAndQueueStub.resolves({retryExchangeName, retryQueueName});
      _assertDeadLetterExchangeAndQueueStub = sinon.stub(rabbit, '_assertDeadLetterExchangeAndQueue');
      _assertDeadLetterExchangeAndQueueStub.resolves(retryExchangeName);
      _decideConsumerRetryStub = sinon.stub(CoinifyRabbit, '_decideConsumerRetry');
      _decideConsumerRetryStub.returns({shouldRetry: false, delaySeconds: 0});
    });

    afterEach(() => {
      _getChannelStub.restore();
      _assertRetryExchangeAndQueueStub.restore();
      _assertDeadLetterExchangeAndQueueStub.restore();
      _decideConsumerRetryStub.restore();
    });

    it('should call options.onError function if provided', async () => {
      options.onError = sinon.stub();
      options.onError.resolves();

      await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(options.onError.calledOnce).to.equal(true);
      expect(options.onError.firstCall.args).to.have.lengthOf(1);
      const argObject = options.onError.firstCall.args[0];

      expect(argObject).to.containSubset({
        err: consumeError,
        context: task.context,
        task,
        willRetry: false,
        delaySeconds: 0
      });
    });

    it('should re-publish to retry queue if _decideConsumerRetry() returns shouldRetry: true', async () => {
      const delaySeconds = 6660;

      _decideConsumerRetryStub.returns({shouldRetry: true, delaySeconds});

      await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(_decideConsumerRetryStub.calledOnce).to.equal(true);
      expect(_decideConsumerRetryStub.firstCall.args).to.deep.equal([task.attempts, options.retry]);

      expect(_assertRetryExchangeAndQueueStub.calledOnce).to.equal(true);
      expect(_assertRetryExchangeAndQueueStub.firstCall.args).to.deep.equal([delaySeconds, _.pick(options, ['exchange', 'queue'])]);

      expect(_assertDeadLetterExchangeAndQueueStub.notCalled).to.equal(true);

      expect(channelPublishStub.calledOnce).to.equal(true);
      expect(channelPublishStub.firstCall.args).to.have.lengthOf(4);
      expect(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
      expect(channelPublishStub.firstCall.args[1]).to.equal(options.queueName);
      expect(channelPublishStub.firstCall.args[2]).to.be.instanceof(Buffer);
      expect(channelPublishStub.firstCall.args[3]).to.deep.equal({BCC: retryQueueName});
      const messageDecoded = JSON.parse(channelPublishStub.firstCall.args[2].toString());
      expect(messageDecoded).to.deep.equal(_.set(task, 'attempts', task.attempts+1));
    });

    it('should re-publish to dead letter queue if _decideConsumerRetry() returns shouldRetry: false', async () => {
      await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(_decideConsumerRetryStub.calledOnce).to.equal(true);
      expect(_decideConsumerRetryStub.firstCall.args).to.deep.equal([task.attempts, options.retry]);

      expect(_assertRetryExchangeAndQueueStub.notCalled).to.equal(true);

      expect(_assertDeadLetterExchangeAndQueueStub.calledOnce).to.equal(true);
      expect(_assertDeadLetterExchangeAndQueueStub.firstCall.args).to.deep.equal([_.pick(options, ['exchange', 'queue'])]);

      expect(channelPublishStub.calledOnce).to.equal(true);
      expect(channelPublishStub.firstCall.args).to.have.lengthOf(4);
      expect(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
      expect(channelPublishStub.firstCall.args[1]).to.equal(options.queueName);
      expect(channelPublishStub.firstCall.args[2]).to.be.instanceof(Buffer);
      expect(channelPublishStub.firstCall.args[3]).to.deep.equal({});
      const messageDecoded = JSON.parse(channelPublishStub.firstCall.args[2].toString());
      expect(messageDecoded).to.deep.equal(_.set(task, 'attempts', task.attempts+1));
    });

    it('should re-publish to dead letter queue if consumeError contains noRetry: true property', async () => {
      const consumeError = new Error('Error that we should not retry on');
      consumeError.noRetry = true;

      // _decideConsumerRetry returns shouldRetry: false, but we want to override it
      const delaySeconds = 6660;
      _decideConsumerRetryStub.returns({shouldRetry: true, delaySeconds});

      await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);

      expect(_assertRetryExchangeAndQueueStub.notCalled).to.equal(true);

      expect(_assertDeadLetterExchangeAndQueueStub.calledOnce).to.equal(true);
      expect(_assertDeadLetterExchangeAndQueueStub.firstCall.args).to.deep.equal([_.pick(options, ['exchange', 'queue'])]);

      expect(channelPublishStub.calledOnce).to.equal(true);
      expect(channelPublishStub.firstCall.args).to.have.lengthOf(4);
      expect(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
      expect(channelPublishStub.firstCall.args[3]).to.deep.equal({});
    });

    it('should reject if type is neither task nor event', async () => {
      const errMsg = 'Invalid type. Given: invalid, allowed: [event, task]';

      expect(rabbit._handleConsumeRejection(message, 'invalid', task, consumeError, options))
        .to.eventually
        .be.rejectedWith(errMsg)
        .and.be.an.instanceOf(Error);
    });
  });
});
