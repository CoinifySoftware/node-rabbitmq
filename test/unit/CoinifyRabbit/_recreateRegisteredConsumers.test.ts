import { expect } from 'chai';
import sinon from 'sinon';
import CoinifyRabbit from '../../../src/CoinifyRabbit';

describe('CoinifyRabbit', () => {

  describe('#_recreateRegisteredConsumers', () => {

    // TODO Test for prefetch option

    let rabbit: any,
      bindEventConsumerStub: sinon.SinonStub,
      bindTaskConsumerStub: sinon.SinonStub;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      bindEventConsumerStub = sinon.stub(rabbit, 'bindEventConsumer');
      bindEventConsumerStub.resolves();
      bindTaskConsumerStub = sinon.stub(rabbit, 'bindTaskConsumer');
      bindTaskConsumerStub.resolves();
    });

    afterEach(() => {
      bindEventConsumerStub.restore();
      bindTaskConsumerStub.restore();
    });

    it('should empty list of registered consumers and re-create them', async () => {
      const eventConsumer1 = { type: 'event', key: 'event-key-1', consumerTag: 'e1', consumeFn: () => undefined, options: { optionsEvent1: true } };
      const eventConsumer2 = { type: 'event', key: 'event-key-2', consumerTag: 'e2', consumeFn: () => undefined, options: { optionsEvent2: true } };
      const eventConsumer3 = { type: 'event', key: 'event-key-3', consumerTag: 'e3', consumeFn: () => undefined, options: { optionsEvent3: true } };
      const taskConsumer1 = { type: 'task', key: 'task-key-1', consumerTag: 't1', consumeFn: () => undefined, options: { optionsTask1: true } };
      const taskConsumer2 = { type: 'task', key: 'task-key-2', consumerTag: 't2', consumeFn: () => undefined, options: { optionsTask2: true } };

      rabbit.consumers = [ eventConsumer1, eventConsumer2, eventConsumer3, taskConsumer1, taskConsumer2 ];

      await rabbit._recreateRegisteredConsumers();

      expect(rabbit.consumers).to.have.lengthOf(0);

      expect(bindEventConsumerStub.callCount).to.equal(3);
      let i = 0;
      for (const { key, consumerTag, consumeFn, options } of [ eventConsumer1, eventConsumer2, eventConsumer3 ]) {
        expect(bindEventConsumerStub.getCall(i).args).to.deep.equal([ key, consumeFn, { ...options, consumerTag } ]);
        i++;
      }

      expect(bindTaskConsumerStub.callCount).to.equal(2);
      i = 0;
      for (const { key, consumerTag, consumeFn, options } of [ taskConsumer1, taskConsumer2 ]) {
        expect(bindTaskConsumerStub.getCall(i).args).to.deep.equal([ key, consumeFn, { ...options, consumerTag } ]);
        i++;
      }
    });

    it('should throw on unknown consumer type', async () => {
      const unknownType = 'not-heard-of-before';
      rabbit.consumers.push({ type: unknownType, key: 'key', consumeFn: () => undefined, options: {} });

      await expect(rabbit._recreateRegisteredConsumers()).to.eventually.be.rejectedWith(unknownType);
    });

  });

});
