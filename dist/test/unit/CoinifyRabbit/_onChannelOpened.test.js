"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_onChannelOpened', () => {
        let _recreateRegisteredConsumersStub, channelPrefetchStub, channel, rabbit;
        const config = { channel: { prefetch: 123 } };
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default(config);
            _recreateRegisteredConsumersStub = sinon_1.default.stub(rabbit, '_recreateRegisteredConsumers');
            _recreateRegisteredConsumersStub.resolves();
            channelPrefetchStub = sinon_1.default.stub();
            channelPrefetchStub.resolves();
            channel = { prefetch: channelPrefetchStub };
        });
        afterEach(() => {
            _recreateRegisteredConsumersStub.restore();
        });
        it('should set the channel prefetch limit', async () => {
            await rabbit._onChannelOpened(channel);
            (0, chai_1.expect)(channelPrefetchStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(channelPrefetchStub.firstCall.args).to.deep.equal([config.channel.prefetch, true]);
        });
        it('should recreate registered consumers', async () => {
            await rabbit._onChannelOpened(channel);
            (0, chai_1.expect)(_recreateRegisteredConsumersStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_recreateRegisteredConsumersStub.firstCall.args).to.have.lengthOf(0);
        });
    });
});
