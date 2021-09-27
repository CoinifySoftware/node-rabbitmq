"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const bootstrap_test_1 = require("../bootstrap.test");
describe('Integration tests', () => {
    describe('#enqueueMessage', () => {
        const context = { myContext: false };
        const serviceName = 'my-test-service';
        const registerConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };
        let rabbit;
        before(() => {
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName } });
        });
        after(async () => {
            await rabbit.shutdown();
        });
        it('should consume enqueued message of type event', () => {
            const eventName = 'my-failed-message' + Math.random();
            const fullEventName = serviceName + '.' + eventName;
            return new Promise(async (resolve) => {
                await rabbit.registerEventConsumer(fullEventName, (c, e) => {
                    (0, chai_1.expect)(e.eventName).to.equal(fullEventName);
                    (0, chai_1.expect)(c).to.deep.equal(context);
                    resolve(undefined);
                }, registerConsumerOptions);
                const routingKey = 'events.my-test-service.' + fullEventName;
                const messageObject = {
                    context,
                    eventName: fullEventName,
                    uuid: 'd51bbaed-1ee8-4bb6-a739-cee5b56ee518',
                    time: 1504865878534,
                    attempts: 12
                };
                await rabbit.enqueueMessage(routingKey, messageObject);
            });
        });
        it('should consume enqueued message of type task', () => {
            const taskName = 'my-task' + Math.random();
            const fullTaskName = serviceName + '.' + taskName;
            return new Promise(async (resolve) => {
                await rabbit.registerTaskConsumer(taskName, (c, t) => {
                    (0, chai_1.expect)(t.taskName).to.equal(fullTaskName);
                    (0, chai_1.expect)(c).to.deep.equal(context);
                    resolve(undefined);
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
                await rabbit.enqueueMessage(queueName, messageObject);
            });
        });
    });
});
