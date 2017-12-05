'use strict';

const CoinifyRabbit = require('../../../lib/CoinifyRabbit'),
  sinon = require('sinon'),
  _ = require('lodash');

describe('CoinifyRabbit', () => {

  describe('#_handleConsumeMessage', () => {

    let _getChannelStub,
      consumeFnStub,
      channelAckStub,
      _handleConsumeRejectionStub,

      rabbit;

    const fullTaskName = 'service.the-task';
    let task,
      message,
      options;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      task = {
        taskName: fullTaskName,
        context: {theContext: true},
        uuid: '1234-4321',
        time: 1122334455
      };
      message = {
        content: JSON.stringify(task)
      };
      options = {
        theOptions: true
      };

      channelAckStub = sinon.stub();
      consumeFnStub = sinon.stub();
      _getChannelStub = sinon.stub(rabbit, '_getChannel');
      _getChannelStub.resolves({ack: channelAckStub});
      _handleConsumeRejectionStub = sinon.stub(rabbit, '_handleConsumeRejection');
    });

    afterEach(() => {
      _getChannelStub.restore();
      _handleConsumeRejectionStub.restore();
    });

    it('should call consumeFn, ack, and return if consumeFn resolves', async () => {
      consumeFnStub.resolves();
      channelAckStub.resolves();

      expect(await rabbit._handleConsumeMessage(message, 'task', options, consumeFnStub)).to.equal(true);

      expect(consumeFnStub.calledOnce).to.equal(true);
      expect(consumeFnStub.firstCall.args).to.deep.equal([task.context, task]);

      expect(channelAckStub.calledOnce).to.equal(true);
      expect(channelAckStub.firstCall.args).to.deep.equal([message]);

      expect(_handleConsumeRejectionStub.notCalled).to.equal(true);
    });

    it('should call consumeFn, ack, log error, and handle rejection if consumeFn rejects', async () => {
      const consumeError = new Error('Consumption rejection');
      consumeFnStub.rejects(consumeError);
      channelAckStub.resolves();
      _handleConsumeRejectionStub.resolves();

      await rabbit._handleConsumeMessage(message, 'task', options, consumeFnStub);

      expect(consumeFnStub.calledOnce).to.equal(true);
      expect(consumeFnStub.firstCall.args).to.deep.equal([task.context, task]);

      expect(channelAckStub.calledOnce).to.equal(true);
      expect(channelAckStub.firstCall.args).to.deep.equal([message]);

      expect(_handleConsumeRejectionStub.calledOnce).to.equal(true);
      expect(_handleConsumeRejectionStub.firstCall.args).to.deep.equal([message, 'task', task, consumeError, options]);
    });

    it('should call onCancel option function if message is null', async () => {
      const onCancelResolution = {theResult: true};
      options.onCancel = sinon.stub();
      options.onCancel.resolves(onCancelResolution);

      expect(await rabbit._handleConsumeMessage(null, 'task', options, consumeFnStub)).to.equal(onCancelResolution);

      expect(options.onCancel.calledOnce).to.equal(true);

      expect(consumeFnStub.notCalled).to.equal(true);
      expect(channelAckStub.notCalled).to.equal(true);
      expect(_handleConsumeRejectionStub.notCalled).to.equal(true);
    });

  });

});
