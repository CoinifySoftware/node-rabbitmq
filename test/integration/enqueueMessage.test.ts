import { expect } from 'chai';
import { SinonSpy } from 'sinon';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import Task from '../../src/messageTypes/Task';
import { createRabbitMQTestInstance, registerChannelPublishSpy, unregisterChannelPublishSpy } from '../bootstrap.test';

describe('Integration tests', () => {

  describe('#enqueueMessage', () => {

    const context = { myContext: false };
    const serviceName = 'my-test-service';
    const registerConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };

    let rabbit: CoinifyRabbit;

    let publishSpy: SinonSpy;

    before(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });
    });

    beforeEach(async () => {
      publishSpy = await registerChannelPublishSpy(rabbit, true);
    });
    afterEach(async () => {
      await unregisterChannelPublishSpy(rabbit, true);
    });

    after(async () => {
      await rabbit.shutdown();
    });

    it('should consume enqueued message of type event', async () => {
      const eventName = 'my-failed-message' + Math.random();
      const fullEventName = serviceName + '.' + eventName;

      await new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, (c, e) => {
          expect(e.eventName).to.equal(fullEventName);
          expect(c).to.deep.equal(context);

          resolve(undefined);
        }, registerConsumerOptions);

        // Set routing key events.' + options.service.name + '.' + eventKey
        const routingKey = 'events.my-test-service.' + fullEventName;
        const messageObject = {
          context,
          eventName: fullEventName,
          uuid: 'd51bbaed-1ee8-4bb6-a739-cee5b56ee518',
          time: 1504865878534,
          attempts: 12
        };

        await rabbit.enqueueMessage(routingKey, messageObject);
      });

      expect(publishSpy.callCount).to.equal(1);
      expect(publishSpy.firstCall.args[3]).to.containSubset({ persistent: true });
    });

    it('should consume enqueued message of type task', async () => {
      const taskName = 'my-task' + Math.random();
      const fullTaskName = serviceName + '.' + taskName;

      await new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, (c, t) => {
          expect(t.taskName).to.equal(fullTaskName);
          expect(c).to.deep.equal(context);

          resolve(undefined);
        }, registerConsumerOptions);

        const queueName = 'tasks.my-test-service.' + taskName;
        const messageObject: Task = {
          context,
          taskName: fullTaskName,
          uuid: 'd51bbaed-1ee8-4bb6-a739-cee5b56ee518',
          time: 1504865878534,
          attempts: 12,
          origin: 'another-service'
        };

        await rabbit.enqueueMessage(queueName, messageObject);
      });

      expect(publishSpy.callCount).to.equal(1);
      expect(publishSpy.firstCall.args[3]).to.containSubset({ persistent: true });
    });
  });
});
