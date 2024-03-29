import { expect } from 'chai';
import CoinifyRabbit from '../../src/CoinifyRabbit';
import { createRabbitMQTestInstance, getChannelPool } from '../bootstrap.test';

describe('Integration tests', () => {
  describe('Connection', () => {

    const serviceName = 'my-test-service';

    const enqueueOptions = { exchange: { autoDelete: true } };
    const consumeOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };

    let rabbit: CoinifyRabbit;

    beforeEach(() => {
      rabbit = createRabbitMQTestInstance({ service: { name: serviceName } });
    });

    afterEach(async () => {
      await rabbit.shutdown();
    });

    it('should reconnect and re-attach registered consumers on unexpected disconnect', () => {
      return new Promise(async (resolve) => {
        let eventConsumed = false;
        let taskConsumed = false;

        const eventContext = { eventContext: true };
        const taskContext = { taskContext: true };

        const _consumed = () => {
          if (eventConsumed && taskConsumed) {
            // If both events were consumed, we can resolve the promise..:!
            resolve(undefined);
          }
        };

        // Attach an event consumer and a task consumer
        await rabbit.registerEventConsumer(serviceName + '.my-event', (c, e) => {
          expect(c).to.deep.equal(eventContext);
          expect(e.eventName).to.equal('my-test-service.my-event');
          eventConsumed = true;
          _consumed();
        }, consumeOptions);

        await rabbit.registerTaskConsumer('my-task', (c, t) => {
          expect(c).to.deep.equal(taskContext);
          expect(t.taskName).to.equal('my-test-service.my-task');
          taskConsumed = true;
          _consumed();
        }, consumeOptions);

        // Now we have attached two consumers, time to fake a disconnect:
        (rabbit as any)._conn.connection.onSocketError(new Error('my err'));

        // Wait a moment
        await new Promise(resolve => setTimeout(resolve, 250));

        // Emit an event and a enqueue a task to check that the consumers have been re-attached
        await rabbit.emitEvent('my-event', eventContext, enqueueOptions);
        await rabbit.enqueueTask('my-test-service.my-task', taskContext, enqueueOptions);
      });
    });

    it('should reconnect and create new channels on unexpected disconnect', async () => {
      const channelPool = getChannelPool(rabbit);

      // Store connection and channel objects so we can check that new ones were made
      const initialConnection = await rabbit._getConnection();
      const initialConsumerChannel = await channelPool.getConsumerChannel();
      const initialPublisherChannel = await channelPool.getPublisherChannel(true);

      // Time to fake a disconnect:
      (rabbit as any)._conn.connection.onSocketError(new Error('my err'));

      // Check that we have new connection and channel objects
      expect(await rabbit._getConnection()).to.not.equal(initialConnection);
      expect(await channelPool.getConsumerChannel()).to.not.equal(initialConsumerChannel);
      expect(await channelPool.getPublisherChannel(true)).to.not.equal(initialPublisherChannel);
    });

    it('should not reconnect on requested shutdown()', async () => {
      // Emit an event to connect and create a channel
      await rabbit.emitEvent('my.event', {}, enqueueOptions);

      // Close again
      await rabbit.shutdown();

      // No connections/channels nor attempts to connect
      expect((rabbit as any)._conn).to.equal(undefined);
      expect((rabbit as any)._getConnectionPromise).to.equal(undefined);
      expect((rabbit as any)._channel).to.equal(undefined);
      expect((rabbit as any)._getChannelPromise).to.equal(undefined);
    });

  });
});
