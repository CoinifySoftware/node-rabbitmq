"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const bootstrap_test_1 = require("../bootstrap.test");
describe('Integration tests', () => {
    describe('Failed', () => {
        let rabbit;
        let taskName, fullTaskName, eventName, fullEventName;
        const serviceName = 'my-test-service', enqueueOptions = { exchange: { autoDelete: true } }, consumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true }, retry: { maxAttempts: 0 } }, failedMessageConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } }, eventContext = { myEventContext: false }, taskContext = { myTaskContext: false }, failingFn = () => {
            throw new Error('event processing function rejected');
        };
        beforeEach(() => {
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName }, defaultLogLevel: 'fatal' });
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
                let eventConsumed = false, taskConsumed = false;
                await rabbit.registerTaskConsumer(taskName, failingFn, consumerOptions);
                await rabbit.registerEventConsumer(fullEventName, failingFn, consumerOptions);
                await rabbit.registerFailedMessageConsumer((q, m) => {
                    if ('taskName' in m) {
                        (0, chai_1.expect)(m.taskName).to.equal(fullTaskName);
                        (0, chai_1.expect)(m.attempts).to.deep.equal(1);
                        (0, chai_1.expect)(m.context).to.deep.equal(taskContext);
                        taskConsumed = true;
                    }
                    if ('eventName' in m) {
                        (0, chai_1.expect)(m.eventName).to.equal(fullEventName);
                        (0, chai_1.expect)(m.attempts).to.deep.equal(1);
                        (0, chai_1.expect)(m.context).to.deep.equal(eventContext);
                        eventConsumed = true;
                    }
                    if (eventConsumed && taskConsumed) {
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
                const fn = (c, m) => {
                    if (punishMeDaddy) {
                        throw new Error('message processing function rejected');
                    }
                    else {
                        (0, chai_1.expect)(m.taskName).to.equal(fullTaskName);
                        (0, chai_1.expect)(m.context).to.deep.equal(taskContext);
                        (0, chai_1.expect)(m.attempts).to.equal(1);
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
