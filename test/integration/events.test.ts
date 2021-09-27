import Event from '../../src/messageTypes/Event';
import { expect } from 'chai';
import _ from 'lodash';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import { createRabbitMQTestInstance, disableFailedMessageQueue, reenableFailedMessageQueue } from '../bootstrap.test';

interface EventContextWithEventNumber {
  eventNumber: number;
}

describe('Integration tests', () => {
  describe('Events', () => {

    let eventName: string, fullEventName: string;
    const context = { myContext: false };
    const serviceName = 'my-test-service';

    const emitEventOptions = { exchange: { autoDelete: true } };
    const registerEventConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };

    let rabbit: CoinifyRabbit;

    before(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });
    });

    beforeEach(() => {
      eventName = 'my-event' + Math.random();
      fullEventName = serviceName + '.' + eventName;
    });

    after(async () => {
      await rabbit.shutdown();
    });

    it('should be able to publish and consume a single event with a single consumer', async () => {
      return new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, async (c, e) => {
          expect(e.eventName).to.equal(fullEventName);
          expect(c).to.deep.equal(context);

          resolve();
        }, registerEventConsumerOptions);
        await rabbit.emitEvent(eventName, context, emitEventOptions);
      });
    });

    it('should be able to publish an event with a pre-defined UUID and timestamp', async () => {
      const uuid = '12341234-1234-1234-1234-123412341234';
      const time = 1511944077916;

      const emitOptions = _.defaults({ uuid, time }, emitEventOptions);

      return new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, async (c, e) => {
          expect(e.uuid).to.equal(uuid);
          expect(e.time).to.equal(time);

          resolve();
        }, registerEventConsumerOptions);
        await rabbit.emitEvent(eventName, context, emitOptions);
      });

    });

    it('should be able to publish and consume multiple events with a single consumer', async () => {
      const eventCount = 3;

      return new Promise(async (resolve) => {
        const contexts = _.map(_.range(eventCount), i => {
          return { eventNumber: i };
        });

        let eventsConsumed = 0;

        await rabbit.registerEventConsumer(fullEventName, async (c, e) => {
          expect(e.eventName).to.equal(fullEventName);
          expect(c).to.deep.equal(contexts[eventsConsumed]);

          eventsConsumed++;

          if (eventsConsumed === eventCount) {
            resolve();
          }
        }, registerEventConsumerOptions);

        for (const context of contexts) {
          await rabbit.emitEvent(eventName, context, emitEventOptions);
        }
      });
    });

    it('should be able to publish a multiple events, load balanced to multiple consumers within the same service', async () => {
      const consumerCount = 3;
      const eventCount = consumerCount * 3;

      const consumerIds = _.range(consumerCount);
      const eventsConsumed: number[] = [];
      const eventsConsumedByConsumer: { [consumerId: number]: number } = {};

      const eventIds = _.range(eventCount);
      const contexts = _.map(eventIds, i => {
        return { eventNumber: i };
      });

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(fullEventName, async (c, e) => { // eslint-disable-line no-loop-func
            expect(e.eventName).to.equal(fullEventName);

            eventsConsumed.push(c.eventNumber);
            eventsConsumedByConsumer[i] = (eventsConsumedByConsumer[i] || 0) + 1;

            if (eventsConsumed.length === eventCount) {
              const eventsConsumedSorted = _.sortBy(eventsConsumed);
              expect(eventsConsumedSorted).to.deep.equal(eventIds);

              resolve();
            }
          }, registerEventConsumerOptions);
        }

        for (const context of contexts) {
          await rabbit.emitEvent(eventName, context, emitEventOptions);
        }
      });
    });

    it('should be able to publish multiple events, broadcast to multiple consumers from different services', async () => {
      const consumerCount = 3;
      const eventCount = 3;

      const consumerIds = _.range(consumerCount);
      // List of IDs of events consumed by all consumers
      const eventsConsumed: number[] = [];
      // List of IDs of events consumed by each consumer
      const eventsConsumedByConsumer: { [consumerId: number]: number[] } = {};

      const eventIds = _.range(eventCount);
      const contexts = _.map(eventIds, i => {
        return { eventNumber: i };
      });

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          const consumeOptions = _.defaultsDeep({}, registerEventConsumerOptions, { service: { name: 'service' + i } });
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(fullEventName, async (c, e) => { // eslint-disable-line no-loop-func
            expect(e.eventName).to.equal(fullEventName);

            eventsConsumed.push(c.eventNumber);
            if (!eventsConsumedByConsumer[i]) {
              eventsConsumedByConsumer[i] = [];
            }
            eventsConsumedByConsumer[i].push(c.eventNumber);

            // If all events have now been consumed by all consumers
            if (eventsConsumed.length === eventCount * consumerCount) {
              // All consumers have consumed something
              expect(_.size(eventsConsumedByConsumer)).to.equal(eventCount);

              // Check that each consumer has consumed all events
              _.forOwn(eventsConsumedByConsumer, (eventsConsumed) => {
                const eventsConsumedSorted = _.sortBy(eventsConsumed);
                expect(eventsConsumedSorted).to.deep.equal(eventIds);
              });

              resolve();
            }
          }, consumeOptions);
        }

        for (const context of contexts) {
          await rabbit.emitEvent(eventName, context, emitEventOptions);
        }
      });
    });

    it('should be able to consume the same event in all instances of the same service', async () => {
      const consumerCount = 3;

      const consumerIds = _.range(consumerCount);
      // List of IDs of events consumed by all consumers
      let eventsConsumed = 0;
      // Number of events consumed by each consumer
      const eventsConsumedByConsumer: { [consumerId: number]: number } = {};

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          const consumeOptions = _.defaultsDeep({}, registerEventConsumerOptions, { service: { name: 'my-service' }, uniqueQueue: true });
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(fullEventName, async (c, e) => { // eslint-disable-line no-loop-func
            expect(e.eventName).to.equal(fullEventName);

            eventsConsumed++;
            if (!eventsConsumedByConsumer[i]) {
              eventsConsumedByConsumer[i] = 0;
            }
            eventsConsumedByConsumer[i]++;

            // If all events have now been consumed by all consumers
            if (eventsConsumed === consumerCount) {
              // All consumers have consumed something
              expect(_.size(eventsConsumedByConsumer)).to.equal(consumerCount);

              // Check that each consumer has consumed the event
              _.forOwn(eventsConsumedByConsumer, eventsConsumed => {
                expect(eventsConsumed).to.equal(1);
              });

              resolve();
            }
          }, consumeOptions);
        }

        await rabbit.emitEvent(eventName, { theContext: true }, emitEventOptions);
      });
    });

    it('should be able to specify events to consume using wildcards', async () => {
      /*
       * 4 consumers:
       * (a) my-test-service.my-event.happened (specific event)
       * (b) my-test-service.my-event.*
       * (c) my-test-service.#
       * (d) #
       *
       * 4 events emitted (consumers in parentheses):
       * (1) my-test-service.my-event.happened      (a+b+c+d)
       * (2) my-test-service.my-event.failed        (b+c+d)
       * (3) my-test-service.another-event.happened (c+d)
       * (4) your-service.shit.hit.the.fan    (d)
       */
      const consumeKey1 = 'my-test-service.my-event.happened';
      const consumeKey2 = 'my-test-service.my-event.*';
      const consumeKey3 = 'my-test-service.#';
      const consumeKey4 = '#';

      const event1 = 'my-test-service.my-event.happened';
      const event2 = 'my-test-service.my-event.failed';
      const event3 = 'my-test-service.another-event.happened';
      const event4 = 'your-service.shit.hit.the.fan';

      const context1 = { eventNumber: 1 };
      const context2 = { eventNumber: 2 };
      const context3 = { eventNumber: 3 };
      const context4 = { eventNumber: 4 };

      const eventsConsumedByConsumer: { [consumerId: number]: { context: EventContextWithEventNumber; event: Event }[] } = { 1: [], 2: [], 3: [], 4: [] };

      const emitEventOptionsWithoutServiceName = _.defaultsDeep({ service: { name: false } }, emitEventOptions);

      await new Promise(async (resolve) => {
        /*
         * Register consumers
         */
        for (const [ i, consumeKey ] of [ [ 1, consumeKey1 ], [ 2, consumeKey2 ], [ 3, consumeKey3 ], [ 4, consumeKey4 ] ] as const) {
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(consumeKey, async (context, event) => { // eslint-disable-line no-loop-func
            // Store that this event was consumed by this consumer
            eventsConsumedByConsumer[i].push({ context, event });

            const totalEventsConsumed = _.flatten(_.values(eventsConsumedByConsumer)).length;
            if (totalEventsConsumed === 10) {
              // All events consumed, resolve here and move on to testes
              resolve(undefined);
            }
          }, registerEventConsumerOptions);
        }

        /*
         * Emit events
         */
        await rabbit.emitEvent(event1, context1, emitEventOptionsWithoutServiceName);
        await rabbit.emitEvent(event2, context2, emitEventOptionsWithoutServiceName);
        await rabbit.emitEvent(event3, context3, emitEventOptionsWithoutServiceName);
        await rabbit.emitEvent(event4, context4, emitEventOptionsWithoutServiceName);
      });

      /*
       * All events have been consumed, total of 10 consumptions. Time to test that the events were received by
       * the expected consumers.
       *
       * Yeah, 1-indexed arrays I know, but it matches the consumer/event numbers
       */
      let { 1: consumed1, 2: consumed2, 3: consumed3, 4: consumed4 } = eventsConsumedByConsumer;
      expect(consumed1).to.have.lengthOf(1);
      expect(consumed2).to.have.lengthOf(2);
      expect(consumed3).to.have.lengthOf(3);
      expect(consumed4).to.have.lengthOf(4);

      // Sort events for easy comparison
      consumed1 = _.sortBy(consumed1, 'context.eventNumber');
      consumed2 = _.sortBy(consumed2, 'context.eventNumber');
      consumed3 = _.sortBy(consumed3, 'context.eventNumber');
      consumed4 = _.sortBy(consumed4, 'context.eventNumber');

      expect(consumed1[0]).to.containSubset({ event: { eventName: event1 }, context: context1 });

      expect(consumed2[0]).to.containSubset({ event: { eventName: event1 }, context: context1 });
      expect(consumed2[1]).to.containSubset({ event: { eventName: event2 }, context: context2 });

      expect(consumed3[0]).to.containSubset({ event: { eventName: event1 }, context: context1 });
      expect(consumed3[1]).to.containSubset({ event: { eventName: event2 }, context: context2 });
      expect(consumed3[2]).to.containSubset({ event: { eventName: event3 }, context: context3 });

      expect(consumed4[0]).to.containSubset({ event: { eventName: event1 }, context: context1 });
      expect(consumed4[1]).to.containSubset({ event: { eventName: event2 }, context: context2 });
      expect(consumed4[2]).to.containSubset({ event: { eventName: event3 }, context: context3 });
      expect(consumed4[3]).to.containSubset({ event: { eventName: event4 }, context: context4 });
    });

    it('should retry an event whose processing function rejected', async () => {
      const eventConsumptionsByConsumer: { [consumerId: number]: number } = { 1: 0, 2: 0, 3: 0, 4: 0 };
      const expectedEventConsumptionsByConsumer = { 1: 5, 2: 1, 3: 1, 4: 1 };

      const maxAttempts = 4;
      const delayMillis = 250;

      await new Promise(async (resolve) => {
        /*
         * Register consumers
         */
        for (const i of [ 1, 2, 3, 4 ]) {

          // Each consumer needs a different service name to create different queues
          const consumeOptions = _.defaultsDeep({
            retry: { backoff: { type: 'fixed', delay: delayMillis / 1000 }, maxAttempts },
            service: { name: 'service' + i }
          }, registerEventConsumerOptions);

          await rabbit.registerEventConsumer(fullEventName, async (context, event) => { // eslint-disable-line no-loop-func
            expect(event.eventName).to.equal(fullEventName);

            // Store that this event was consumed by this consumer
            eventConsumptionsByConsumer[i]++;


            if (i === 1 && eventConsumptionsByConsumer[i] <= maxAttempts) {
              throw new Error('Failing for the first consumer');
            }

            if (_.isEqual(eventConsumptionsByConsumer, expectedEventConsumptionsByConsumer)) {
              resolve(undefined);
            }

          }, consumeOptions);
        }

        await rabbit.emitEvent(eventName, context, emitEventOptions);
      });

      expect(eventConsumptionsByConsumer).to.deep.equal(expectedEventConsumptionsByConsumer);
    }).timeout(10000);

    it('should respect prefetch setting for event consumer', async () => {
      const prefetch = 3;
      const taskCount = 3 * prefetch;
      const consumeTime = 250;

      const consumerOptions = _.defaultsDeep({ consumer: { prefetch } }, registerEventConsumerOptions);
      const consumeTimestamps: number[] = [];

      await new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, async () => {
          await new Promise(resolve => setTimeout(resolve, consumeTime));
          consumeTimestamps.push(Date.now());

          if (consumeTimestamps.length === taskCount) {
            resolve(undefined);
          }
        }, consumerOptions);

        await Promise.all(new Array(taskCount).fill(undefined).map(async () => rabbit.emitEvent(eventName, context, emitEventOptions)));
      });

      // Average timestamp of each group of consumptions
      const timestampMeans = _.chunk(consumeTimestamps, prefetch).map(_.mean);

      for (let i = 1; i < timestampMeans.length; i++) {
        const diff = timestampMeans[i] - timestampMeans[i - 1];
        expect(diff).to.be.at.least(consumeTime - 100).and.at.most(consumeTime + 100);
      }
    });

    it('should use custom onError function if event consumption rejects', async () => {
      const err = new Error('The error');
      const context = { theContext: true };

      disableFailedMessageQueue(rabbit);

      await new Promise(async (resolve, reject) => {
        await rabbit.registerEventConsumer(fullEventName, async () => {
          throw err;
        }, {
          ...registerEventConsumerOptions,
          onError: async params => {
            try {
              expect(params).to.containSubset({
                err,
                context,
                event: {
                  eventName: fullEventName,
                  context
                }
              });

              resolve(undefined);
            } catch (err) {
              reject(err);
            }
          }
        });

        await rabbit.emitEvent(eventName, context, emitEventOptions);
      });

      // Wait a bit to ensure consumer closes
      await new Promise(resolve => setTimeout(resolve, 25));

      reenableFailedMessageQueue(rabbit);
    });

    it('should use custom onError function if event consumption throws', async () => {
      const err = new Error('The error');
      const context = { theContext: true };

      disableFailedMessageQueue(rabbit);

      await new Promise(async (resolve, reject) => {
        await rabbit.registerEventConsumer(fullEventName, () => {
          throw err;
        }, {
          ...registerEventConsumerOptions,
          onError: async params => {
            try {
              expect(params).to.containSubset({
                err,
                context,
                event: {
                  eventName: fullEventName,
                  context
                }
              });

              resolve(undefined);
            } catch (err) {
              reject(err);
            }
          }
        });

        await rabbit.emitEvent(eventName, context, emitEventOptions);
      });

      // Wait a bit to ensure consumer closes
      await new Promise(resolve => setTimeout(resolve, 25));

      reenableFailedMessageQueue(rabbit);
    });

  });
});
