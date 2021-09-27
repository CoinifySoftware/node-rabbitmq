"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_onChannelClosed', () => {
        let _connectWithBackoffStub, rabbit;
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default();
            _connectWithBackoffStub = sinon_1.default.stub(rabbit, '_connectWithBackoff');
            _connectWithBackoffStub.resolves();
        });
        afterEach(() => {
            _connectWithBackoffStub.restore();
        });
        it('should do nothing if shutdown was initiated', async () => {
            rabbit.isShuttingDown = true;
            await rabbit._onChannelClosed();
            (0, chai_1.expect)(_connectWithBackoffStub.notCalled).to.equal(true);
        });
        it('should do nothing on unexpected closing if there are no registered consumers', async () => {
            rabbit.isShuttingDown = false;
            await rabbit._onChannelClosed();
            (0, chai_1.expect)(_connectWithBackoffStub.notCalled).to.equal(true);
        });
        it('should call _connectWithBackoff() on unexpected closing if there are registered consumers', async () => {
            rabbit.isShuttingDown = false;
            rabbit.consumers.push({});
            await rabbit._onChannelClosed();
            (0, chai_1.expect)(_connectWithBackoffStub.calledOnce).to.equal(true);
        });
    });
});
