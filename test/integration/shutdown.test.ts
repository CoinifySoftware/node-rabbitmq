import { expect } from 'chai';
import _ from 'lodash';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import { createRabbitMQTestInstance } from '../bootstrap.test';

describe('Integration tests', () => {
  describe('Graceful shutdown', () => {

    let taskName: string, eventName: string, fullTaskName: string, fullEventName: string;
    const serviceName = 'my-test-service';

    const enqueueOptions = { exchange: { autoDelete: true } };
    const consumeOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };

    let rabbit: CoinifyRabbit;

    beforeEach(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName }, defaultLogLevel: 'fatal' });
      taskName = 'my-task' + Math.random();
      eventName = 'my-event' + Math.random();
      fullTaskName = serviceName + '.' + taskName;
      fullEventName = serviceName + '.' + eventName;
    });

    it('should wait for all consumer functions to finish before returning', async () => {
      let eventConsumeCount = 0;
      let taskConsumeCount = 0;

      const startTime = Date.now();

      /*
       * - Emit event / enqueue task
       * - Perform graceful shutdown without timeout
       * - wait until both event and task consumption has finished
       */
      await new Promise(async (resolve) => {
        const _consumed = async () => {
          if (eventConsumeCount === 1 && taskConsumeCount === 1) {
            // If both events were consumed, we can resolve the promise..:!
            resolve(undefined);
          }
        };

        // Attach an event consumer and a task consumer
        await rabbit.registerEventConsumer(fullEventName, async () => {
          await new Promise(resolve => setTimeout(resolve, 500));
          eventConsumeCount += 1;
          await _consumed();
        }, consumeOptions);

        await rabbit.registerTaskConsumer(taskName, async () => {
          await new Promise(resolve => setTimeout(resolve, 1000));
          taskConsumeCount += 1;
          await _consumed();
        }, consumeOptions);

        // Emit event and enqueue task
        await rabbit.emitEvent(eventName, {}, enqueueOptions);
        await rabbit.enqueueTask(fullTaskName, {}, enqueueOptions);

        // Wait a little bit to allow the messages to propagate through RabbitMQ and end up in our consumers
        await new Promise(resolve => setTimeout(resolve, 100));

        // Instruct module to shutdown gracefully
        await rabbit.shutdown();
      });

      const elapsedMillis = Date.now() - startTime;
      // At least 1 seconds should have passed (my-task takes 1 second to consume)
      expect(elapsedMillis).to.be.at.least(1000);

      expect(eventConsumeCount).to.equal(1);
      expect(taskConsumeCount).to.equal(1);

      /*
       * Connect to rabbitMQ again, checking that no events or tasks are in the queue
       */
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });

      await rabbit.registerEventConsumer(fullEventName, async () => {
        eventConsumeCount += 1;
      }, consumeOptions);

      await rabbit.registerTaskConsumer(taskName, async () => {
        taskConsumeCount += 1;
      }, consumeOptions);

      await new Promise(resolve => setTimeout(resolve, 1000));

      expect(eventConsumeCount).to.equal(1);
      expect(taskConsumeCount).to.equal(1);

      await rabbit.shutdown();
    }).timeout(5000);

    it('should wait for consumer functions to finish within the given timeout period', async () => {
      let eventConsumeCount = 0;
      let taskConsumeCount = 0;

      const startTime = Date.now();

      /*
       * - Emit event / enqueue task
       * - Perform graceful shutdown with timeout
       * - consume event in time, consume task should not finish
       */
      await rabbit.registerEventConsumer(fullEventName, async () => {
        await new Promise(resolve => setTimeout(resolve, 250));
        eventConsumeCount += 1;
      }, consumeOptions);

      await rabbit.registerTaskConsumer(taskName, async () => {
        await new Promise(resolve => setTimeout(resolve, 1000));
        taskConsumeCount += 1;
      }, consumeOptions);

      // Emit event and enqueue task
      await rabbit.emitEvent(eventName, {}, enqueueOptions);
      await rabbit.enqueueTask(fullTaskName, {}, enqueueOptions);

      // Wait a little bit to allow the messages to propagate through RabbitMQ and end up in our consumers
      await new Promise(resolve => setTimeout(resolve, 100));

      // Instruct module to shutdown gracefully, allow 0.5 second for timeout
      // This should be enough time to consume the event, but not the task
      await rabbit.shutdown(500);

      const elapsedMillis = Date.now() - startTime;
      // At least 1 seconds should have passed (the timeout specified in the shutdown() call)
      expect(elapsedMillis).to.be.at.least(500);
      // No more than 3 seconds should have passed (the consumption time of the my-task task)
      expect(elapsedMillis).to.be.at.most(1000);

      expect(eventConsumeCount).to.equal(1);
      expect(taskConsumeCount).to.equal(0);

      /*
       * Connect to rabbitMQ again, checking that no events are in the queue, but a task should be
       */
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });

      await rabbit.registerEventConsumer(fullEventName, async () => {
        eventConsumeCount += 1;
      }, consumeOptions);

      await rabbit.registerTaskConsumer(taskName, async () => {
        taskConsumeCount += 1;
      }, consumeOptions);

      await new Promise(resolve => setTimeout(resolve, 500));

      expect(eventConsumeCount).to.equal(1);
      expect(taskConsumeCount).to.equal(1);

      await rabbit.shutdown();
    }).timeout(3000);

    it('should just close connection and channel if no consumer functions are registered', async () => {
      await rabbit._getChannel();
      const startTime = Date.now();

      await rabbit.shutdown();

      const elapsedMillis = Date.now() - startTime;
      expect(elapsedMillis).to.be.at.most(100);
    });

    it('should not consume anything after shutdown() has returned', async () => {
      let consumeCount = 0;

      // disable autoDelete for queue, as we need to enqueue a task which should persist until the otherRabbit
      // consumer is ready. Delete the queue automatically after 1 second of incativity.
      const myConsumeOptions = _.defaultsDeep({ queue: { expires: 1000, autoDelete: false } }, consumeOptions);

      await rabbit.registerTaskConsumer(taskName, async () => {
        consumeCount++;
      }, myConsumeOptions);

      const startTime = Date.now();
      await rabbit.shutdown();

      // Shutdown should be fairly quick, as no consumer functions are running
      const elapsedMillis = Date.now() - startTime;
      expect(elapsedMillis).to.be.at.most(100);

      const otherRabbit = createRabbitMQTestInstance({ service: { name: serviceName } });

      // Emit an event which should not be consumed
      await otherRabbit.enqueueTask(fullTaskName, {}, enqueueOptions);

      // Wait a little bit to allow the messages to propagate through RabbitMQ and end up in our consumers
      await new Promise(resolve => setTimeout(resolve, 100));

      // Nothing consumed
      expect(consumeCount).to.equal(0);

      // Consume using other connection
      let consumeCountNewConsumer = 0;
      await otherRabbit.registerTaskConsumer(taskName, async () => {
        consumeCountNewConsumer++;
      }, myConsumeOptions);

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 500));

      // Task consumed by new consumer
      expect(consumeCount).to.equal(0);
      expect(consumeCountNewConsumer).to.equal(1);

      await otherRabbit.shutdown();
    });

  });
});
