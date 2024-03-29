import { expect } from 'chai';
import chunk from 'lodash.chunk';
import { SinonSpy } from 'sinon';
import { RegisterTaskConsumerOptions } from '../..';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import { createRabbitMQTestInstance, disableFailedMessageQueue, reenableFailedMessageQueue, registerChannelPublishSpy, unregisterChannelPublishSpy } from '../bootstrap.test';

describe('Integration tests', () => {
  describe('Tasks', () => {

    let taskName: string, fullTaskName: string;
    const context = { myContext: false };
    const serviceName = 'my-test-service';

    const enqueueTaskOptions = { exchange: { autoDelete: true } };
    const registerTaskConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };

    let rabbit: CoinifyRabbit;

    let publishSpy: SinonSpy;

    before(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });
    });

    beforeEach( async () => {
      taskName = 'my-task' + Math.random();
      fullTaskName = serviceName + '.' + taskName;
      publishSpy = await registerChannelPublishSpy(rabbit, true);
    });
    afterEach(async () => {
      await unregisterChannelPublishSpy(rabbit, true);
    });

    after(async () => {
      await rabbit.shutdown();
    });

    it('should be able to enqueue and consume a single task with a single consumer', () => {
      return new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, (c, t) => {
          expect(t.taskName).to.equal(fullTaskName);
          expect(c).to.deep.equal(context);

          resolve(undefined);
        }, registerTaskConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
      });
    });

    it('should publish a task as a persistent message', async () => {
      await rabbit.enqueueTask(taskName, context, enqueueTaskOptions);

      expect(publishSpy.callCount).to.equal(1);
      expect(publishSpy.firstCall.args[3]).to.containSubset({ persistent: true });
    });

    it('should be able to enqueue a task with a pre-defined UUID and timestamp', () => {
      const uuid = '12341234-1234-1234-1234-123412341234';
      const time = 1511944077916;

      const enqueueOptions = { ...enqueueTaskOptions, uuid, time };

      return new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, (c, t) => {
          expect(t.uuid).to.equal(uuid);
          expect(t.time).to.equal(time);

          resolve(undefined);
        }, registerTaskConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, context, enqueueOptions);
      });
    });

    it('should be able to enqueue and consume multiple tasks with a single consumer', () => {
      const taskCount = 3;

      return new Promise(async (resolve) => {
        const contexts = Array.from({ length: taskCount }, (_, taskNumber) => ({ taskNumber }));

        let tasksConsumed = 0;

        await rabbit.registerTaskConsumer(taskName, (c, t) => {
          expect(t.taskName).to.equal(fullTaskName);
          expect(c).to.deep.equal(contexts[tasksConsumed]);

          tasksConsumed++;

          if (tasksConsumed === taskCount) {
            resolve(undefined);
          }
        }, registerTaskConsumerOptions);

        for (const context of contexts) {
          await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
        }
      });
    });

    it('should be able to enqueue a multiple tasks, load balanced to multiple consumers within the same service', () => {
      const consumerCount = 3;
      const taskCount = consumerCount * 3;

      const consumerIds = Array.from({ length: consumerCount }, (_, i) => i);
      const tasksConsumed: number[] = [];
      const tasksConsumedByConsumer: { [consumerId: number]: number } = {};

      const taskIds = Array.from({ length: taskCount }, (_, i) => i);
      const contexts = taskIds.map(i => ({ taskNumber: i }));

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          await rabbit.registerTaskConsumer(taskName, (c, t) => { // eslint-disable-line no-loop-func
            expect(t.taskName).to.equal(fullTaskName);

            tasksConsumed.push(c.taskNumber);
            tasksConsumedByConsumer[i] = (tasksConsumedByConsumer[i] || 0) + 1;

            if (tasksConsumed.length === taskCount) {
              tasksConsumed.sort();
              expect(tasksConsumed).to.deep.equal(taskIds);

              resolve(undefined);
            }
          }, registerTaskConsumerOptions);
        }

        for (const context of contexts) {
          await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
        }
      });
    });

    it('should be able to consume the same task in all instances of the same service', () => {
      const consumerCount = 3;

      const consumerIds = Array.from({ length: consumerCount }, (_, i) => i);
      // List of IDs of tasks consumed by all consumers
      let tasksConsumed = 0;
      // Number of tasks consumed by each consumer
      const tasksConsumedByConsumer: { [consumerId: number]: number } = {};

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          const consumeOptions = { ...registerTaskConsumerOptions, uniqueQueue: true };
          await rabbit.registerTaskConsumer(taskName, (c, e) => { // eslint-disable-line no-loop-func
            expect(e.taskName).to.equal(fullTaskName);

            tasksConsumed++;
            if (!tasksConsumedByConsumer[i]) {
              tasksConsumedByConsumer[i] = 0;
            }
            tasksConsumedByConsumer[i]++;

            // If all tasks have now been consumed by all consumers
            if (tasksConsumed === consumerCount) {
              // All consumers have consumed something
              expect(Object.values(tasksConsumedByConsumer)).to.have.lengthOf(consumerCount);

              // Check that each consumer has consumed the task
              expect(Object.values(tasksConsumedByConsumer).every(tasksConsumed => tasksConsumed === 1)).to.equal(true);

              resolve(undefined);
            }
          }, consumeOptions);
        }

        await rabbit.enqueueTask(fullTaskName, { theContext: true }, enqueueTaskOptions);
      });
    });

    it('should retry a task whose processing function rejected', () => {
      return new Promise(async (resolve) => {

        // Retry 4 times with 0.25 second delay
        const delayMillis = 250;
        const maxAttempts = 4;
        const consumeOptions: RegisterTaskConsumerOptions = { ...registerTaskConsumerOptions, retry: { backoff: { type: 'fixed', delay: delayMillis / 1000 }, maxAttempts } };
        let startTime = Date.now();
        let attempt = 0;

        await rabbit.registerTaskConsumer(taskName, (c, e) => {
          expect(e.taskName).to.equal(fullTaskName);
          expect(e.attempts).to.equal(attempt);
          expect(c).to.deep.equal(context);

          const endTime = Date.now();
          const elapsed = endTime - startTime;
          startTime = endTime;

          // Check the delay timing (not for first attempt, which should be immediate)
          if (attempt > 0) {
            expect(elapsed).to.be.at.least(delayMillis);
            // Allow 200 ms for error
            expect(elapsed).to.be.at.most(delayMillis + 200);
          }

          attempt++;

          if (attempt <= maxAttempts) {
            throw new Error('Processing function rejected');
          } else {
            resolve(undefined);
          }
        }, consumeOptions);
        await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
      });

    }).timeout(10000);

    it('should respect prefetch setting for task consumer', async () => {
      const prefetch = 3;
      const taskCount = 3 * prefetch;
      const consumeTime = 250;

      const consumerOptions = { ...registerTaskConsumerOptions, consumer: { prefetch } };
      const consumeTimestamps: number[] = [];

      await new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, async () => {
          await new Promise(resolve => setTimeout(resolve, consumeTime));
          consumeTimestamps.push(Date.now());

          if ( consumeTimestamps.length === taskCount ) {
            resolve(undefined);
          }
        }, consumerOptions);

        await Promise.all(new Array(taskCount).fill(undefined).map(() => rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions)));
      });

      // Average timestamp of each group of consumptions
      const timestampMeans = chunk(consumeTimestamps, prefetch).map(timestamps => timestamps.reduce((a, b) => a + b, 0) / timestamps.length);

      for (let i = 1; i < timestampMeans.length; i++) {
        const diff = timestampMeans[i] - timestampMeans[i-1];
        expect(diff).to.be.at.least(consumeTime - 100).and.at.most(consumeTime + 100);
      }
    });

    it('should be able to enqueue a delayed task', () => {
      const delayMillis = 1000;
      const enqueueOptions = Object.assign({ delayMillis }, enqueueTaskOptions);
      let enqueueTime: number;

      return new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, (c, t) => {
          const consumeTime = Date.now();

          expect(t.taskName).to.equal(fullTaskName);
          expect(t.delayMillis).to.equal(delayMillis);
          expect(c).to.deep.equal(context);
          expect(consumeTime - enqueueTime).to.be.closeTo(delayMillis, 50);

          resolve(undefined);
        }, registerTaskConsumerOptions);
        enqueueTime = Date.now();
        await rabbit.enqueueTask(fullTaskName, context, enqueueOptions);
      });
    });

    it('should use custom onError function if task consumption rejects', async () => {
      const err = new Error('The error');
      const context = { theContext: true };

      // Unwrap publish function before calling disableFailedMessageQueue() below
      await unregisterChannelPublishSpy(rabbit, true);
      await disableFailedMessageQueue(rabbit);

      await new Promise(async (resolve, reject) => {
        await rabbit.registerTaskConsumer(taskName, () => {
          throw err;
        }, {
          ...registerTaskConsumerOptions,
          onError: params => {
            try {
              expect(params).to.containSubset({
                err,
                context,
                task: {
                  taskName: fullTaskName,
                  context
                }
              });

              resolve(undefined);
            } catch (err) {
              reject(err);
            }
          }
        });


        await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
      });

      // Wait a bit to ensure consumer closes
      await new Promise(resolve => setTimeout(resolve, 25));

      await reenableFailedMessageQueue(rabbit);
    });

    it('should use custom onError function if task consumption throws', async () => {
      const err = new Error('The error');
      const context = { theContext: true };

      // Unwrap publish function before calling disableFailedMessageQueue() below
      await unregisterChannelPublishSpy(rabbit, true);
      await disableFailedMessageQueue(rabbit);

      await new Promise(async (resolve, reject) => {
        await rabbit.registerTaskConsumer(taskName, () => {
          throw err;
        }, {
          ...registerTaskConsumerOptions,
          onError: params => {
            try {
              expect(params).to.containSubset({
                err,
                context,
                task: {
                  taskName: fullTaskName,
                  context
                }
              });

              resolve(undefined);
            } catch (err) {
              reject(err);
            }
          }
        });

        await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
      });

      // Wait a bit to ensure consumer closes
      await new Promise(resolve => setTimeout(resolve, 25));

      await reenableFailedMessageQueue(rabbit);
    });

  });
});
