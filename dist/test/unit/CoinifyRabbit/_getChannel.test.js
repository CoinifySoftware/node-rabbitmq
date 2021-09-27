"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const events_1 = __importDefault(require("events"));
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_getChannel', () => {
        let _getConnectionStub, _onChannelOpenedStub, _onChannelClosedStub, createChannelStub, amqpConnection, rabbit;
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default();
            createChannelStub = sinon_1.default.stub();
            amqpConnection = { createChannel: createChannelStub };
            _getConnectionStub = sinon_1.default.stub(rabbit, '_getConnection');
            _getConnectionStub.resolves(amqpConnection);
            _onChannelOpenedStub = sinon_1.default.stub(rabbit, '_onChannelOpened');
            _onChannelOpenedStub.resolves();
            _onChannelClosedStub = sinon_1.default.stub(rabbit, '_onChannelClosed');
            _onChannelClosedStub.resolves();
        });
        afterEach(() => {
            _getConnectionStub.restore();
            _onChannelOpenedStub.restore();
            _onChannelClosedStub.restore();
        });
        it('should create and cache a channel', async () => {
            const channel = new events_1.default();
            createChannelStub.onCall(0).resolves(channel);
            createChannelStub.rejects(new Error('amqplib.createChannel() called more than once.'));
            (0, chai_1.expect)(await rabbit._getChannel()).to.equal(channel);
            (0, chai_1.expect)(createChannelStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(await rabbit._getChannel()).to.equal(channel);
            (0, chai_1.expect)(createChannelStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_onChannelOpenedStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_onChannelOpenedStub.firstCall.args).to.deep.equal([channel]);
        });
        it('should create and cache a single connection, even if creating the connection is not instantaneous', async () => {
            const channel = new events_1.default();
            createChannelStub.onCall(0).resolves(channel);
            createChannelStub.rejects(new Error('amqplib.createChannel() called more than once.'));
            const [conn1, conn2, conn3] = await Promise.all([rabbit._getChannel(), rabbit._getChannel(), rabbit._getChannel()]);
            (0, chai_1.expect)(conn1).to.equal(channel);
            (0, chai_1.expect)(conn2).to.equal(channel);
            (0, chai_1.expect)(conn3).to.equal(channel);
            (0, chai_1.expect)(createChannelStub.calledOnce).to.equal(true);
            const [conn4, conn5, conn6] = await Promise.all([rabbit._getChannel(), rabbit._getChannel(), rabbit._getChannel()]);
            (0, chai_1.expect)(conn4).to.equal(channel);
            (0, chai_1.expect)(conn5).to.equal(channel);
            (0, chai_1.expect)(conn6).to.equal(channel);
            (0, chai_1.expect)(createChannelStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_onChannelOpenedStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(_onChannelOpenedStub.firstCall.args).to.deep.equal([channel]);
        });
        it('should create a channel, and drop it if an error occurs', async () => {
            const channel = new events_1.default();
            createChannelStub.resolves(channel);
            (0, chai_1.expect)(await rabbit._getChannel()).to.equal(channel);
            (0, chai_1.expect)(createChannelStub.calledOnce).to.equal(true);
            channel.emit('error', new Error('Test channel error'));
            await new Promise(resolve => setImmediate(resolve));
            (0, chai_1.expect)(await rabbit._getChannel()).to.equal(channel);
            (0, chai_1.expect)(createChannelStub.calledTwice).to.equal(true);
            (0, chai_1.expect)(_onChannelOpenedStub.calledTwice).to.equal(true);
            (0, chai_1.expect)(_onChannelOpenedStub.firstCall.args).to.deep.equal([channel]);
            (0, chai_1.expect)(_onChannelOpenedStub.secondCall.args).to.deep.equal([channel]);
        });
        it('should create a channel, and then delete cached channel and call _onChannelClosed() if channel emits \'close\' event', async () => {
            const channel = new events_1.default();
            createChannelStub.resolves(channel);
            (0, chai_1.expect)(await rabbit._getChannel()).to.equal(channel);
            channel.emit('close');
            await new Promise(resolve => setImmediate(resolve));
            (0, chai_1.expect)(_onChannelClosedStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(rabbit._channel).to.equal(undefined);
        });
    });
});
