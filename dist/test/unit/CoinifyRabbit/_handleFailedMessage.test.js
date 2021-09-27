"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_handleFailedMessage', () => {
        let _getChannelStub, consumeFnStub, channelAckStub, channelNackStub, rabbit;
        const fullTaskName = 'service.the-task';
        let task, message, options;
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default();
            task = {
                taskName: fullTaskName,
                context: { theContext: true },
                uuid: '1234-4321',
                time: 1122334455
            };
            message = {
                content: JSON.stringify(task),
                fields: { routingKey: 'path-to-message-queue' }
            };
            options = {
                theOptions: true
            };
            channelAckStub = sinon_1.default.stub();
            channelNackStub = sinon_1.default.stub();
            consumeFnStub = sinon_1.default.stub();
            _getChannelStub = sinon_1.default.stub(rabbit, '_getChannel');
            _getChannelStub.resolves({ ack: channelAckStub, nack: channelNackStub });
        });
        afterEach(() => {
            _getChannelStub.restore();
        });
        it('should call consumeFn and ack if consumeFn resolves', async () => {
            consumeFnStub.resolves();
            channelAckStub.resolves();
            await rabbit._handleFailedMessage(message, options, consumeFnStub);
            (0, chai_1.expect)(consumeFnStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(consumeFnStub.firstCall.args).to.deep.equal([message.fields.routingKey, task]);
            (0, chai_1.expect)(channelAckStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(channelAckStub.firstCall.args).to.deep.equal([message]);
            (0, chai_1.expect)(channelNackStub.notCalled).to.equal(true);
        });
        it('should call consumeFn and nack if consumeFn rejects', async () => {
            const consumeError = new Error('Consumption rejection');
            consumeFnStub.rejects(consumeError);
            channelAckStub.resolves();
            await rabbit._handleFailedMessage(message, options, consumeFnStub);
            (0, chai_1.expect)(consumeFnStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(consumeFnStub.firstCall.args).to.deep.equal([message.fields.routingKey, task]);
            (0, chai_1.expect)(channelAckStub.notCalled).to.equal(true);
            (0, chai_1.expect)(channelNackStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(channelNackStub.firstCall.args).to.deep.equal([message]);
        });
        it('should call onCancel option function if message is null', async () => {
            const onCancelResolution = { theResult: true };
            options.onCancel = sinon_1.default.stub();
            options.onCancel.resolves(onCancelResolution);
            (0, chai_1.expect)(await rabbit._handleFailedMessage(null, options, consumeFnStub)).to.equal(onCancelResolution);
            (0, chai_1.expect)(options.onCancel.calledOnce).to.equal(true);
            (0, chai_1.expect)(consumeFnStub.notCalled).to.equal(true);
            (0, chai_1.expect)(channelAckStub.notCalled).to.equal(true);
            (0, chai_1.expect)(channelNackStub.notCalled).to.equal(true);
        });
    });
});
