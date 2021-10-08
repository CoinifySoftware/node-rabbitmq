"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_handleConsumeRejection', () => {
        let _getChannelStub, channelPublishStub, _assertRetryExchangeAndQueueStub, _assertDeadLetterExchangeAndQueueStub, _decideConsumerRetryStub, rabbit;
        let message, task, options;
        const consumeError = new Error('The error');
        const retryExchangeName = 'the-exchange';
        const retryQueueName = 'the-retry-queue';
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default({ defaultLogLevel: 'fatal' });
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
            channelPublishStub = sinon_1.default.stub();
            channelPublishStub.resolves(true);
            _getChannelStub = sinon_1.default.stub(rabbit, '_getChannel');
            _getChannelStub.resolves({ publish: channelPublishStub });
            _assertRetryExchangeAndQueueStub = sinon_1.default.stub(rabbit, '_assertRetryExchangeAndQueue');
            _assertRetryExchangeAndQueueStub.resolves({ retryExchangeName, retryQueueName });
            _assertDeadLetterExchangeAndQueueStub = sinon_1.default.stub(rabbit, '_assertDeadLetterExchangeAndQueue');
            _assertDeadLetterExchangeAndQueueStub.resolves(retryExchangeName);
            _decideConsumerRetryStub = sinon_1.default.stub(CoinifyRabbit_1.default, '_decideConsumerRetry');
            _decideConsumerRetryStub.returns({ shouldRetry: false, delaySeconds: 0 });
        });
        afterEach(() => {
            _getChannelStub.restore();
            _assertRetryExchangeAndQueueStub.restore();
            _assertDeadLetterExchangeAndQueueStub.restore();
            _decideConsumerRetryStub.restore();
        });
        it('should re-publish to retry queue if _decideConsumerRetry() returns shouldRetry: true', async () => {
            const delaySeconds = 6660;
            _decideConsumerRetryStub.returns({ shouldRetry: true, delaySeconds });
            await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);
            (0, chai_1.expect)(_decideConsumerRetryStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_decideConsumerRetryStub.firstCall.args).to.deep.equal([task.attempts, options.retry]);
            (0, chai_1.expect)(_assertRetryExchangeAndQueueStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_assertRetryExchangeAndQueueStub.firstCall.args).to.deep.equal([delaySeconds, { exchange: options.exchange, queue: options.queue }]);
            (0, chai_1.expect)(_assertDeadLetterExchangeAndQueueStub.notCalled).to.equal(true);
            (0, chai_1.expect)(channelPublishStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(channelPublishStub.firstCall.args).to.have.lengthOf(4);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[1]).to.equal(options.queueName);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[2]).to.be.instanceof(Buffer);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[3]).to.deep.equal({ BCC: retryQueueName });
            const messageDecoded = JSON.parse(channelPublishStub.firstCall.args[2].toString());
            (0, chai_1.expect)(messageDecoded).to.deep.equal({ ...task, attempts: task.attempts + 1 });
        });
        it('should re-publish to dead letter queue if _decideConsumerRetry() returns shouldRetry: false', async () => {
            await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);
            (0, chai_1.expect)(_decideConsumerRetryStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_decideConsumerRetryStub.firstCall.args).to.deep.equal([task.attempts, options.retry]);
            (0, chai_1.expect)(_assertRetryExchangeAndQueueStub.notCalled).to.equal(true);
            (0, chai_1.expect)(_assertDeadLetterExchangeAndQueueStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_assertDeadLetterExchangeAndQueueStub.firstCall.args).to.deep.equal([{ exchange: options.exchange, queue: options.queue }]);
            (0, chai_1.expect)(channelPublishStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(channelPublishStub.firstCall.args).to.have.lengthOf(4);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[1]).to.equal(options.queueName);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[2]).to.be.instanceof(Buffer);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[3]).to.deep.equal({});
            const messageDecoded = JSON.parse(channelPublishStub.firstCall.args[2].toString());
            (0, chai_1.expect)(messageDecoded).to.deep.equal({ ...task, attempts: task.attempts + 1 });
        });
        it('should re-publish to dead letter queue if consumeError contains noRetry: true property', async () => {
            const consumeError = new Error('Error that we should not retry on');
            consumeError.noRetry = true;
            const delaySeconds = 6660;
            _decideConsumerRetryStub.returns({ shouldRetry: true, delaySeconds });
            await rabbit._handleConsumeRejection(message, 'task', task, consumeError, options);
            (0, chai_1.expect)(_assertRetryExchangeAndQueueStub.notCalled).to.equal(true);
            (0, chai_1.expect)(_assertDeadLetterExchangeAndQueueStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_assertDeadLetterExchangeAndQueueStub.firstCall.args).to.deep.equal([{ exchange: options.exchange, queue: options.queue }]);
            (0, chai_1.expect)(channelPublishStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(channelPublishStub.firstCall.args).to.have.lengthOf(4);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[0]).to.equal(retryExchangeName);
            (0, chai_1.expect)(channelPublishStub.firstCall.args[3]).to.deep.equal({});
        });
        it('should reject if type is neither task nor event', async () => {
            const errMsg = 'Invalid type. Given: invalid, allowed: [event, task]';
            await (0, chai_1.expect)(rabbit._handleConsumeRejection(message, 'invalid', task, consumeError, options))
                .to.eventually
                .be.rejectedWith(errMsg)
                .and.be.an.instanceOf(Error);
        });
    });
});
