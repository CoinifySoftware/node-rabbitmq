'use strict';

const CoinifyRabbit = require('../../lib/CoinifyRabbit');
const { createRabbitMQTestInstance } = require('../bootstrap.test');

describe('Integration tests', () => {

  describe('#enqueueMessage', () => {

    const context = { myContext: false };
    const serviceName = 'my-test-service';
    const registerConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };
    const enqueueMessageOptions = { exchange: { autoDelete: true } };

    let rabbit;
    before(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });
    });

    after(async () => {
      await rabbit.shutdown();
    });

    it('should consume enqueued message of type event', () => {
      const eventName = 'my-failed-message' + Math.random();
      const fullEventName = serviceName + '.' + eventName;

      return new Promise(async (resolve, reject) => {
        await rabbit.registerEventConsumer(fullEventName, async (c, e) => {
          expect(e.eventName).to.equal(fullEventName);
          expect(c).to.deep.equal(context);

          resolve();
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

        await rabbit.enqueueMessage(routingKey, messageObject, enqueueMessageOptions);
      });
    });

    it('should consume enqueued message of type task', () => {
      const taskName = 'my-task' + Math.random();
      const fullTaskName = serviceName + '.' + taskName;

      return new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, async (c, t) => {
          expect(t.taskName).to.equal(fullTaskName);
          expect(c).to.deep.equal(context);

          resolve();
        }, registerConsumerOptions);

        const queueName = 'tasks.my-test-service.' + taskName;
        const messageObject = {
          context,
          taskName: fullTaskName,
          uuid: 'd51bbaed-1ee8-4bb6-a739-cee5b56ee518',
          time: 1504865878534,
          attempts: 12,
          origin: 'another-service'
        };

        await rabbit.enqueueMessage(queueName, messageObject, enqueueMessageOptions);
      });
    });
  });
});
