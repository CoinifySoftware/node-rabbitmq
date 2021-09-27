"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const bootstrap_test_1 = require("../bootstrap.test");
describe('Integration tests', () => {
    describe('Connection', () => {
        const serviceName = 'my-test-service';
        const enqueueOptions = { exchange: { autoDelete: true } };
        const consumeOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };
        let rabbit;
        beforeEach(() => {
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName } });
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
                        resolve(undefined);
                    }
                };
                const initialConnection = await rabbit._getConnection();
                const initialChannel = await rabbit._getChannel();
                await rabbit.registerEventConsumer(serviceName + '.my-event', (c, e) => {
                    (0, chai_1.expect)(c).to.deep.equal(eventContext);
                    (0, chai_1.expect)(e.eventName).to.equal('my-test-service.my-event');
                    eventConsumed = true;
                    _consumed();
                }, consumeOptions);
                await rabbit.registerTaskConsumer('my-task', (c, t) => {
                    (0, chai_1.expect)(c).to.deep.equal(taskContext);
                    (0, chai_1.expect)(t.taskName).to.equal('my-test-service.my-task');
                    taskConsumed = true;
                    _consumed();
                }, consumeOptions);
                rabbit._conn.connection.onSocketError(new Error('my err'));
                await new Promise(resolve => setTimeout(resolve, 250));
                (0, chai_1.expect)(await rabbit._getConnection()).to.not.equal(initialConnection);
                (0, chai_1.expect)(await rabbit._getChannel()).to.not.equal(initialChannel);
                await rabbit.emitEvent('my-event', eventContext, enqueueOptions);
                await rabbit.enqueueTask('my-test-service.my-task', taskContext, enqueueOptions);
            });
        });
        it('should not reconnect on requested shutdown()', async () => {
            await rabbit._getChannel();
            await rabbit.shutdown();
            (0, chai_1.expect)(rabbit._conn).to.equal(undefined);
            (0, chai_1.expect)(rabbit._getConnectionPromise).to.equal(undefined);
            (0, chai_1.expect)(rabbit._channel).to.equal(undefined);
            (0, chai_1.expect)(rabbit._getChannelPromise).to.equal(undefined);
        });
    });
});
