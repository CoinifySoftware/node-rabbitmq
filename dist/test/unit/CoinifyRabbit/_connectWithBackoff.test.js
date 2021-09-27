"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const backoff_1 = __importDefault(require("backoff"));
const chai_1 = require("chai");
const events_1 = __importDefault(require("events"));
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_connectWithBackoff', () => {
        let _getChannelStub, backoffFibonacciStub, rabbit;
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default();
            _getChannelStub = sinon_1.default.stub(rabbit, '_getChannel');
            _getChannelStub.resolves();
            backoffFibonacciStub = sinon_1.default.stub(backoff_1.default, 'fibonacci');
            backoffFibonacciStub.throws(new Error('Should not be called'));
        });
        afterEach(() => {
            _getChannelStub.restore();
            backoffFibonacciStub.restore();
        });
        it('should call _getChannel() with backoff', async () => {
            const backoffAttempts = 5;
            const rejectError = new Error('Error');
            _getChannelStub.rejects(rejectError);
            _getChannelStub.onCall(backoffAttempts - 1).resolves();
            const fibonacci = new events_1.default();
            fibonacci.backoff = sinon_1.default.stub();
            fibonacci.backoff.returns();
            backoffFibonacciStub.returns(fibonacci);
            await rabbit._connectWithBackoff();
            (0, chai_1.expect)(backoffFibonacciStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(fibonacci.backoff.calledOnce).to.equal(true);
            (0, chai_1.expect)(_getChannelStub.notCalled).to.equal(true);
            for (let i = 0; i < backoffAttempts; i++) {
                fibonacci.emit('ready', i, 0);
            }
            await new Promise(resolve => process.nextTick(resolve));
            (0, chai_1.expect)(_getChannelStub.callCount).to.equal(backoffAttempts);
            (0, chai_1.expect)(fibonacci.backoff.callCount).to.equal(backoffAttempts);
        });
    });
});
