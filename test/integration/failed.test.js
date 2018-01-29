'use strict';

const CoinifyRabbit = require('../../lib/CoinifyRabbit');

describe('Integration tests', () => {
  describe('Failed', () => {

    let rabbit, taskName, fullTaskName, eventName, fullEventName;
    const serviceName = 'my-test-service',
      enqueueOptions = {exchange: {autoDelete: true}},
      consumerOptions = {exchange: {autoDelete: true}, queue: {autoDelete: true}, retry: {maxAttempts: 0}},
      failedMessageConsumerOptions = {exchange: {autoDelete: true}, queue: {autoDelete: true}},
      eventContext = {myEventContext: false},
      taskContext = {myTaskContext: false},
      failingFn = async () => { throw new Error('event processing function rejected'); };

    beforeEach(async() => {
      rabbit = new CoinifyRabbit({service: {name: serviceName}});
      taskName = 'my-task' + Math.random();
      fullTaskName = serviceName + '.' + taskName;
      eventName = 'my-event' + Math.random();
      fullEventName = serviceName + '.' + eventName;
    });

    afterEach(async () => {
      await rabbit.shutdown();
    });

    it('should be able to consume any type of failed message in a failed message consumer', async () => {
      return new Promise(async (resolve) => {
        let eventConsumed = false,
          taskConsumed = false;

        await rabbit.registerTaskConsumer(taskName, failingFn, consumerOptions);
        await rabbit.registerEventConsumer(fullEventName, failingFn, consumerOptions);
        await rabbit.registerFailedMessageConsumer(async (c, t) => {
          if (t.taskName){
            expect(t.taskName).to.equal(fullTaskName);
            expect(t.attempts).to.deep.equal(1);
            expect(c).to.deep.equal(taskContext);
            taskConsumed = true;  
          }
          if (t.eventName){
            expect(t.eventName).to.equal(fullEventName);
            expect(t.attempts).to.deep.equal(1);
            expect(c).to.deep.equal(eventContext);
            eventConsumed = true;  
          }

          if (eventConsumed && taskConsumed) {
            // If both events were consumed, we can resolve the promise..:!
            resolve();
          }
        }, failedMessageConsumerOptions);
        await rabbit.enqueueTask(fullTaskName, taskContext, enqueueOptions);
        await rabbit.emitEvent(eventName, eventContext, enqueueOptions);
      });
    });
  });
});