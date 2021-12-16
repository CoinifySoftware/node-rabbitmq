import { expect } from 'chai';
import chunk from 'lodash.chunk';
import { SinonSpy } from 'sinon';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import Event, { RegisterEventConsumerOptions } from '../../src/messageTypes/Event';
import { createRabbitMQTestInstance, disableFailedMessageQueue, reenableFailedMessageQueue, registerChannelPublishSpy, unregisterChannelPublishSpy } from '../bootstrap.test';

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

    let publishSpy: SinonSpy;

    before(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });
    });

    beforeEach( async () => {
      eventName = 'my-event' + Math.random();
      fullEventName = serviceName + '.' + eventName;
      publishSpy = await registerChannelPublishSpy(rabbit, true);
    });
    afterEach(async () => {
      await unregisterChannelPublishSpy(rabbit, true);
    });

    after(async () => {
      await rabbit.shutdown();
    });

    it('should be able to publish and consume a single event with a single consumer', () => {
      return new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, (c, e) => {
          expect(e.eventName).to.equal(fullEventName);
          expect(c).to.deep.equal(context);

          resolve(undefined);
        }, registerEventConsumerOptions);
        await rabbit.emitEvent(eventName, context, emitEventOptions);
      });
    });

    it('should publish an event as a persistent message', async () => {
      await rabbit.emitEvent(eventName, context, emitEventOptions);

      expect(publishSpy.callCount).to.equal(1);
      expect(publishSpy.firstCall.args[3]).to.containSubset({ persistent: true });
    });

    it('should be able to publish an event with a pre-defined UUID and timestamp', () => {
      const uuid = '12341234-1234-1234-1234-123412341234';
      const time = 1511944077916;

      const emitOptions = { ...emitEventOptions, uuid, time };

      return new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, (c, e) => {
          expect(e.uuid).to.equal(uuid);
          expect(e.time).to.equal(time);

          resolve(undefined);
        }, registerEventConsumerOptions);
        await rabbit.emitEvent(eventName, context, emitOptions);
      });

    });

    it('should be able to publish and consume multiple events with a single consumer', () => {
      const eventCount = 3;

      return new Promise(async (resolve) => {
        const contexts = Array.from({ length: eventCount }, (_, eventNumber) => ({ eventNumber }));

        let eventsConsumed = 0;

        await rabbit.registerEventConsumer(fullEventName, (c, e) => {
          expect(e.eventName).to.equal(fullEventName);
          expect(c).to.deep.equal(contexts[eventsConsumed]);

          eventsConsumed++;

          if (eventsConsumed === eventCount) {
            resolve(undefined);
          }
        }, registerEventConsumerOptions);

        for (const context of contexts) {
          await rabbit.emitEvent(eventName, context, emitEventOptions);
        }
      });
    });

    it('should be able to publish a multiple events, load balanced to multiple consumers within the same service', () => {
      const consumerCount = 3;
      const eventCount = consumerCount * 3;

      const consumerIds = Array.from({ length: consumerCount }, (_, i) => i);
      const eventsConsumed: number[] = [];
      const eventsConsumedByConsumer: { [consumerId: number]: number } = {};

      const eventIds = Array.from({ length: eventCount }, (_, i) => i);
      const contexts = eventIds.map(eventNumber => ({ eventNumber }));

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(fullEventName, (c, e) => { // eslint-disable-line no-loop-func
            expect(e.eventName).to.equal(fullEventName);

            eventsConsumed.push(c.eventNumber);
            eventsConsumedByConsumer[i] = (eventsConsumedByConsumer[i] || 0) + 1;

            if (eventsConsumed.length === eventCount) {
              const eventsConsumedSorted = eventsConsumed.sort();
              expect(eventsConsumedSorted).to.deep.equal(eventIds);

              resolve(undefined);
            }
          }, registerEventConsumerOptions);
        }

        for (const context of contexts) {
          await rabbit.emitEvent(eventName, context, emitEventOptions);
        }
      });
    });

    it('should be able to publish multiple events, broadcast to multiple consumers from different services', () => {
      const consumerCount = 3;
      const eventCount = 3;

      const consumerIds = Array.from({ length: consumerCount }, (_, i) => i);
      // List of IDs of events consumed by all consumers
      const eventsConsumed: number[] = [];
      // List of IDs of events consumed by each consumer
      const eventsConsumedByConsumer: { [consumerId: number]: number[] } = {};

      const eventIds = Array.from({ length: eventCount }, (_, i) => i);
      const contexts = eventIds.map(eventNumber => ({ eventNumber }));

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          const consumeOptions = { ...registerEventConsumerOptions, service: { name: 'service' + i } };
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(fullEventName, (c, e) => { // eslint-disable-line no-loop-func
            expect(e.eventName).to.equal(fullEventName);

            eventsConsumed.push(c.eventNumber);
            if (!eventsConsumedByConsumer[i]) {
              eventsConsumedByConsumer[i] = [];
            }
            eventsConsumedByConsumer[i].push(c.eventNumber);

            // If all events have now been consumed by all consumers
            if (eventsConsumed.length === eventCount * consumerCount) {
              // All consumers have consumed something
              expect(Object.values(eventsConsumedByConsumer)).to.have.lengthOf(eventCount);

              // Check that each consumer has consumed all events
              for (const consumerId in eventsConsumedByConsumer) {
                const eventsConsumedSorted = [ ...eventsConsumedByConsumer[consumerId] ].sort();
                expect(eventsConsumedSorted).to.deep.equal(eventIds);
              }

              resolve(undefined);
            }
          }, consumeOptions);
        }

        for (const context of contexts) {
          await rabbit.emitEvent(eventName, context, emitEventOptions);
        }
      });
    });

    it('should be able to consume the same event in all instances of the same service', () => {
      const consumerCount = 3;

      const consumerIds = Array.from({ length: consumerCount }, (_, i) => i);
      // List of IDs of events consumed by all consumers
      let eventsConsumed = 0;
      // Number of events consumed by each consumer
      const eventsConsumedByConsumer: { [consumerId: number]: number } = {};

      return new Promise(async (resolve) => {
        for (const i of consumerIds) {
          const consumeOptions = { ...registerEventConsumerOptions, service: { name: 'my-service' }, uniqueQueue: true };
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(fullEventName, (c, e) => { // eslint-disable-line no-loop-func
            expect(e.eventName).to.equal(fullEventName);

            eventsConsumed++;
            if (!eventsConsumedByConsumer[i]) {
              eventsConsumedByConsumer[i] = 0;
            }
            eventsConsumedByConsumer[i]++;

            // If all events have now been consumed by all consumers
            if (eventsConsumed === consumerCount) {
              // All consumers have consumed something
              expect(Object.values(eventsConsumedByConsumer)).to.have.lengthOf(consumerCount);

              // Check that each consumer has consumed the event
              expect(Object.values(eventsConsumedByConsumer).every(eventsConsumed => eventsConsumed === 1)).to.equal(true);

              resolve(undefined);
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

      const emitEventOptionsWithoutServiceName = { ...emitEventOptions, service: { name: '' } };

      await new Promise(async (resolve) => {
        /*
         * Register consumers
         */
        for (const [ i, consumeKey ] of [ [ 1, consumeKey1 ], [ 2, consumeKey2 ], [ 3, consumeKey3 ], [ 4, consumeKey4 ] ] as const) {
          await rabbit.registerEventConsumer<EventContextWithEventNumber>(consumeKey, (context, event) => { // eslint-disable-line no-loop-func
            // Store that this event was consumed by this consumer
            eventsConsumedByConsumer[i].push({ context, event });

            const totalEventsConsumed = Object.values(eventsConsumedByConsumer).flat().length;
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
      const { 1: consumed1, 2: consumed2, 3: consumed3, 4: consumed4 } = eventsConsumedByConsumer;
      expect(consumed1).to.have.lengthOf(1);
      expect(consumed2).to.have.lengthOf(2);
      expect(consumed3).to.have.lengthOf(3);
      expect(consumed4).to.have.lengthOf(4);

      // Sort events for easy comparison
      consumed1.sort((a, b) => a.context.eventNumber - b.context.eventNumber);
      consumed2.sort((a, b) => a.context.eventNumber - b.context.eventNumber);
      consumed3.sort((a, b) => a.context.eventNumber - b.context.eventNumber);
      consumed4.sort((a, b) => a.context.eventNumber - b.context.eventNumber);

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
          const consumeOptions: RegisterEventConsumerOptions = {
            ...registerEventConsumerOptions,
            retry: { backoff: { type: 'fixed', delay: delayMillis / 1000 }, maxAttempts },
            service: { name: 'service' + i }
          };

          await rabbit.registerEventConsumer(fullEventName, (context, event) => { // eslint-disable-line no-loop-func
            expect(event.eventName).to.equal(fullEventName);

            // Store that this event was consumed by this consumer
            eventConsumptionsByConsumer[i]++;


            if (i === 1 && eventConsumptionsByConsumer[i] <= maxAttempts) {
              throw new Error('Failing for the first consumer');
            }

            if (JSON.stringify(eventConsumptionsByConsumer) === JSON.stringify(expectedEventConsumptionsByConsumer)) {
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

      const consumerOptions = { ...registerEventConsumerOptions, consumer: { prefetch } };
      const consumeTimestamps: number[] = [];

      await new Promise(async (resolve) => {
        await rabbit.registerEventConsumer(fullEventName, async () => {
          await new Promise(resolve => setTimeout(resolve, consumeTime));
          consumeTimestamps.push(Date.now());

          if (consumeTimestamps.length === taskCount) {
            resolve(undefined);
          }
        }, consumerOptions);

        await Promise.all(new Array(taskCount).fill(undefined).map(() => rabbit.emitEvent(eventName, context, emitEventOptions)));
      });

      // Average timestamp of each group of consumptions
      const timestampMeans = chunk(consumeTimestamps, prefetch).map(timestamps => timestamps.reduce((a, b) => a + b, 0) / timestamps.length);

      for (let i = 1; i < timestampMeans.length; i++) {
        const diff = timestampMeans[i] - timestampMeans[i - 1];
        expect(diff).to.be.at.least(consumeTime - 100).and.at.most(consumeTime + 100);
      }
    });

    it('should use custom onError function if event consumption rejects', async () => {
      const err = new Error('The error');
      const context = { theContext: true };

      // Unwrap publish function before calling disableFailedMessageQueue() below
      await unregisterChannelPublishSpy(rabbit, true);
      await disableFailedMessageQueue(rabbit);

      await new Promise(async (resolve, reject) => {
        await rabbit.registerEventConsumer(fullEventName, () => {
          throw err;
        }, {
          ...registerEventConsumerOptions,
          onError: params => {
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

      await reenableFailedMessageQueue(rabbit);
    });

    it('should use custom onError function if event consumption throws', async () => {
      const err = new Error('The error');
      const context = { theContext: true };

      // Unwrap publish function before calling disableFailedMessageQueue() below
      await unregisterChannelPublishSpy(rabbit, true);
      await disableFailedMessageQueue(rabbit);

      await new Promise(async (resolve, reject) => {
        await rabbit.registerEventConsumer(fullEventName, () => {
          throw err;
        }, {
          ...registerEventConsumerOptions,
          onError: params => {
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

      await reenableFailedMessageQueue(rabbit);
    });

  });
});
