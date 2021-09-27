import { expect } from 'chai';
import sinon from 'sinon';
import CoinifyRabbit from '../../../src/CoinifyRabbit';

describe('CoinifyRabbit', () => {

  describe('#_handleFailedMessage', () => {

    let _getChannelStub: sinon.SinonStub,
      consumeFnStub: sinon.SinonStub,
      channelAckStub: sinon.SinonStub,
      channelNackStub: sinon.SinonStub,

      rabbit: CoinifyRabbit;

    const fullTaskName = 'service.the-task';
    let task: any,
      message: any,
      options: any;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

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

      channelAckStub = sinon.stub();
      channelNackStub = sinon.stub();
      consumeFnStub = sinon.stub();
      _getChannelStub = sinon.stub(rabbit, '_getChannel');
      _getChannelStub.resolves({ ack: channelAckStub, nack: channelNackStub });
    });

    afterEach(() => {
      _getChannelStub.restore();
    });

    it('should call consumeFn and ack if consumeFn resolves', async () => {
      consumeFnStub.resolves();
      channelAckStub.resolves();
      await (rabbit as any)._handleFailedMessage(message, options, consumeFnStub);
      expect(consumeFnStub.calledOnce).to.equal(true);
      expect(consumeFnStub.firstCall.args).to.deep.equal([ message.fields.routingKey, task ]);
      expect(channelAckStub.calledOnce).to.equal(true);
      expect(channelAckStub.firstCall.args).to.deep.equal([ message ]);
      expect(channelNackStub.notCalled).to.equal(true);
    });

    it('should call consumeFn and nack if consumeFn rejects', async () => {
      const consumeError = new Error('Consumption rejection');
      consumeFnStub.rejects(consumeError);
      channelAckStub.resolves();

      await (rabbit as any)._handleFailedMessage(message, options, consumeFnStub);
      expect(consumeFnStub.calledOnce).to.equal(true);
      expect(consumeFnStub.firstCall.args).to.deep.equal([ message.fields.routingKey, task ]);
      expect(channelAckStub.notCalled).to.equal(true);
      expect(channelNackStub.calledOnce).to.equal(true);
      expect(channelNackStub.firstCall.args).to.deep.equal([ message ]);
    });

    it('should call onCancel option function if message is null', async () => {
      const onCancelResolution = { theResult: true };
      options.onCancel = sinon.stub();
      options.onCancel.resolves(onCancelResolution);

      expect(await (rabbit as any)._handleFailedMessage(null, options, consumeFnStub)).to.equal(onCancelResolution);
      expect(options.onCancel.calledOnce).to.equal(true);
      expect(consumeFnStub.notCalled).to.equal(true);
      expect(channelAckStub.notCalled).to.equal(true);
      expect(channelNackStub.notCalled).to.equal(true);
    });
  });
});
