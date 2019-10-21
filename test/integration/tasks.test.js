'use strict';

const CoinifyRabbit = require('../../lib/CoinifyRabbit');

describe('Integration tests', () => {
  describe('Tasks', () => {

    let taskName, fullTaskName;
    const context = { myContext: false };
    const serviceName = 'my-test-service';

    const enqueueTaskOptions = { exchange: { autoDelete: true } };
    const registerTaskConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };

    let rabbit;

    before(() => {
      rabbit = new CoinifyRabbit({ service: { name: serviceName } });
    });

    beforeEach(() => {
      taskName = 'my-task' + Math.random();
      fullTaskName = serviceName + '.' + taskName;
    });

    after(async () => {
      await rabbit.shutdown();
    });

    it('should be able to enqueue and consume a single task with a single consumer', async () => {
      return new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, async (c, t) => {
          expect(t.taskName).to.equal(fullTaskName);
          expect(c).to.deep.equal(context);

          resolve();
        }, registerTaskConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
      });
    });

    it('should be able to enqueue a task with a pre-defined UUID and timestamp', async () => {
      const uuid = '12341234-1234-1234-1234-123412341234';
      const time = 1511944077916;

      const enqueueOptions = _.defaults({ uuid, time }, enqueueTaskOptions);

      return new Promise(async (resolve, reject) => {
        await rabbit.registerTaskConsumer(taskName, async (c, t) => {
          expect(t.uuid).to.equal(uuid);
          expect(t.time).to.equal(time);

          resolve();
        }, registerTaskConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, context, enqueueOptions);
      });
    });

    it('should be able to enqueue and consume multiple tasks with a single consumer', async () => {
      const taskCount = 3;

      return new Promise(async (resolve) => {
        const contexts = _.map(_.range(taskCount), i => {
          return { taskNumber: i };
        });

        let tasksConsumed = 0;

        await rabbit.registerTaskConsumer(taskName, async (c, t) => {
          expect(t.taskName).to.equal(fullTaskName);
          expect(c).to.deep.equal(contexts[tasksConsumed]);

          tasksConsumed++;

          if (tasksConsumed === taskCount) {
            resolve();
          }
        }, registerTaskConsumerOptions);

        for (const context of contexts) {
          await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
        }
      });
    });

    it('should be able to enqueue a multiple tasks, load balanced to multiple consumers within the same service', async () => {
      const consumerCount = 3;
      const taskCount = consumerCount * 3;

      const consumerIds = _.range(consumerCount);
      const tasksConsumed = [];
      const tasksConsumedByConsumer = {};

      const taskIds = _.range(taskCount);
      const contexts = _.map(taskIds, i => {
        return { taskNumber: i };
      });

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          await rabbit.registerTaskConsumer(taskName, (c, t) => { // eslint-disable-line no-loop-func
            expect(t.taskName).to.equal(fullTaskName);

            tasksConsumed.push(c.taskNumber);
            tasksConsumedByConsumer[i] = (tasksConsumedByConsumer[i] || 0) + 1;

            if (tasksConsumed.length === taskCount) {
              const tasksConsumedSorted = _.sortBy(tasksConsumed);
              expect(tasksConsumedSorted).to.deep.equal(taskIds);

              resolve();
            }
          }, registerTaskConsumerOptions);
        }

        for (const context of contexts) {
          await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
        }
      });
    });

    it('should retry a task whose processing function rejected', async () => {
      return new Promise(async (resolve) => {

        // Retry 4 times with 0.75 second delay
        const delayMillis = 750;
        const maxAttempts = 4;
        const consumeOptions = _.defaultsDeep({ retry: { backoff: { type: 'fixed', delay: delayMillis / 1000 }, maxAttempts } }, registerTaskConsumerOptions);
        let startTime = Date.now();
        let attempt = 0;

        await rabbit.registerTaskConsumer(taskName, async (c, e) => {
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
            resolve();
          }
        }, consumeOptions);
        await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
      });

    }).timeout(10000);

    it('should respect prefetch setting for task consumer', async () => {
      const prefetch = 3;
      const taskCount = 3 * prefetch;
      const consumeTime = 250;

      const consumerOptions = _.defaultsDeep({ consumer: { prefetch } }, registerTaskConsumerOptions);
      const consumeTimestamps = [];

      await new Promise(async (resolve) => {
        await rabbit.registerTaskConsumer(taskName, async (c, t) => {
          await new Promise(resolve => setTimeout(resolve, consumeTime));
          consumeTimestamps.push(Date.now());

          if ( consumeTimestamps.length === taskCount ) {
            resolve();
          }
        }, consumerOptions);

        await Promise.all(new Array(taskCount).fill().map(async () => rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions)));
      });

      // Average timestamp of each group of consumptions
      const timestampMeans = _.chunk(consumeTimestamps, prefetch).map(_.mean);

      for (let i = 1; i < timestampMeans.length; i++) {
        const diff = timestampMeans[i] - timestampMeans[i-1];
        expect(diff).to.be.at.least(consumeTime - 100).and.at.most(consumeTime + 100);
      }
    });

  });
});