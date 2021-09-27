import { expect } from 'chai';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import { TaskConsumerFunction } from '../../src/messageTypes/Task';
import { createRabbitMQTestInstance } from '../bootstrap.test';

describe('Integration tests', () => {
  describe('Failed', () => {

    let rabbit: CoinifyRabbit;
    let taskName: string, fullTaskName: string, eventName: string, fullEventName: string;
    const serviceName = 'my-test-service',
      enqueueOptions = { exchange: { autoDelete: true } },
      consumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true }, retry: { maxAttempts: 0 } },
      failedMessageConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } },
      eventContext = { myEventContext: false },
      taskContext = { myTaskContext: false },
      failingFn = () => {
        throw new Error('event processing function rejected');
      };

    beforeEach(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName }, defaultLogLevel: 'fatal' });
      taskName = 'my-task' + Math.random();
      fullTaskName = serviceName + '.' + taskName;
      eventName = 'my-event' + Math.random();
      fullEventName = serviceName + '.' + eventName;
    });

    afterEach(async () => {
      await rabbit.shutdown();
    });

    it('should be able to consume any type of failed message in a failed message consumer', () => {
      return new Promise(async (resolve) => {
        let eventConsumed = false,
          taskConsumed = false;

        await rabbit.registerTaskConsumer(taskName, failingFn, consumerOptions);
        await rabbit.registerEventConsumer(fullEventName, failingFn, consumerOptions);
        await rabbit.registerFailedMessageConsumer((q, m) => {
          if ('taskName' in m){
            expect(m.taskName).to.equal(fullTaskName);
            expect(m.attempts).to.deep.equal(1);
            expect(m.context).to.deep.equal(taskContext);
            taskConsumed = true;
          }
          if ('eventName' in m){
            expect(m.eventName).to.equal(fullEventName);
            expect(m.attempts).to.deep.equal(1);
            expect(m.context).to.deep.equal(eventContext);
            eventConsumed = true;
          }

          if (eventConsumed && taskConsumed) {
            // If both events were consumed, we can resolve the promise..:!
            resolve(undefined);
          }
        }, failedMessageConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, taskContext, enqueueOptions);
        await rabbit.emitEvent(eventName, eventContext, enqueueOptions);
      });
    }).timeout(3000);

    it('should be able to reenqueue a failed message', () => {
      return new Promise(async (resolve) => {
        let punishMeDaddy = true;
        const fn: TaskConsumerFunction = (c, m) => {
          if (punishMeDaddy){
            throw new Error('message processing function rejected');
          } else {
            expect(m.taskName).to.equal(fullTaskName);
            expect(m.context).to.deep.equal(taskContext);
            expect(m.attempts).to.equal(1);
            resolve(undefined);
          }
        };
        await rabbit.registerTaskConsumer(taskName, fn, consumerOptions);
        await rabbit.registerFailedMessageConsumer(async (q, m) => {
          punishMeDaddy = false;
          await rabbit.enqueueMessage(q, m);
        }, failedMessageConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, taskContext, enqueueOptions);
      });
    });
  });
});
