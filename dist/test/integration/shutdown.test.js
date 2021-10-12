"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const bootstrap_test_1 = require("../bootstrap.test");
describe('Integration tests', () => {
    describe('Graceful shutdown', () => {
        let taskName, eventName, fullTaskName, fullEventName;
        const serviceName = 'my-test-service';
        const enqueueOptions = { exchange: { autoDelete: true } };
        const consumeOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };
        let rabbit;
        beforeEach(() => {
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName }, defaultLogLevel: 'fatal' });
            taskName = 'my-task' + Math.random();
            eventName = 'my-event' + Math.random();
            fullTaskName = serviceName + '.' + taskName;
            fullEventName = serviceName + '.' + eventName;
        });
        it('should wait for all consumer functions to finish before returning', async () => {
            let eventConsumeCount = 0;
            let taskConsumeCount = 0;
            const startTime = Date.now();
            await new Promise(async (resolve) => {
                const _consumed = () => {
                    if (eventConsumeCount === 1 && taskConsumeCount === 1) {
                        resolve(undefined);
                    }
                };
                await rabbit.registerEventConsumer(fullEventName, async () => {
                    await new Promise(resolve => setTimeout(resolve, 500));
                    eventConsumeCount += 1;
                    _consumed();
                }, consumeOptions);
                await rabbit.registerTaskConsumer(taskName, async () => {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    taskConsumeCount += 1;
                    _consumed();
                }, consumeOptions);
                await rabbit.emitEvent(eventName, {}, enqueueOptions);
                await rabbit.enqueueTask(fullTaskName, {}, enqueueOptions);
                await new Promise(resolve => setTimeout(resolve, 100));
                await rabbit.shutdown();
            });
            const elapsedMillis = Date.now() - startTime;
            (0, chai_1.expect)(elapsedMillis).to.be.at.least(1000);
            (0, chai_1.expect)(eventConsumeCount).to.equal(1);
            (0, chai_1.expect)(taskConsumeCount).to.equal(1);
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName } });
            await rabbit.registerEventConsumer(fullEventName, () => {
                eventConsumeCount += 1;
            }, consumeOptions);
            await rabbit.registerTaskConsumer(taskName, () => {
                taskConsumeCount += 1;
            }, consumeOptions);
            await new Promise(resolve => setTimeout(resolve, 1000));
            (0, chai_1.expect)(eventConsumeCount).to.equal(1);
            (0, chai_1.expect)(taskConsumeCount).to.equal(1);
            await rabbit.shutdown();
        }).timeout(5000);
        it('should wait for consumer functions to finish within the given timeout period', async () => {
            let eventConsumeCount = 0;
            let taskConsumeCount = 0;
            const startTime = Date.now();
            await rabbit.registerEventConsumer(fullEventName, async () => {
                await new Promise(resolve => setTimeout(resolve, 250));
                eventConsumeCount += 1;
            }, consumeOptions);
            await rabbit.registerTaskConsumer(taskName, async () => {
                await new Promise(resolve => setTimeout(resolve, 1000));
                taskConsumeCount += 1;
            }, consumeOptions);
            await rabbit.emitEvent(eventName, {}, enqueueOptions);
            await rabbit.enqueueTask(fullTaskName, {}, enqueueOptions);
            await new Promise(resolve => setTimeout(resolve, 100));
            await rabbit.shutdown(500);
            const elapsedMillis = Date.now() - startTime;
            (0, chai_1.expect)(elapsedMillis).to.be.at.least(500);
            (0, chai_1.expect)(elapsedMillis).to.be.at.most(1000);
            (0, chai_1.expect)(eventConsumeCount).to.equal(1);
            (0, chai_1.expect)(taskConsumeCount).to.equal(0);
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName } });
            await rabbit.registerEventConsumer(fullEventName, () => {
                eventConsumeCount += 1;
            }, consumeOptions);
            await rabbit.registerTaskConsumer(taskName, () => {
                taskConsumeCount += 1;
            }, consumeOptions);
            await new Promise(resolve => setTimeout(resolve, 500));
            (0, chai_1.expect)(eventConsumeCount).to.equal(1);
            (0, chai_1.expect)(taskConsumeCount).to.equal(1);
            await rabbit.shutdown();
        }).timeout(3000);
        it('should just close connection and channel if no consumer functions are registered', async () => {
            await rabbit._getChannel();
            const startTime = Date.now();
            await rabbit.shutdown();
            const elapsedMillis = Date.now() - startTime;
            (0, chai_1.expect)(elapsedMillis).to.be.at.most(100);
        });
        it('should not consume anything after shutdown() has returned', async () => {
            let consumeCount = 0;
            const myConsumeOptions = { ...consumeOptions, queue: { expires: 1000, autoDelete: false } };
            await rabbit.registerTaskConsumer(taskName, () => {
                consumeCount++;
            }, myConsumeOptions);
            const startTime = Date.now();
            await rabbit.shutdown();
            const elapsedMillis = Date.now() - startTime;
            (0, chai_1.expect)(elapsedMillis).to.be.at.most(100);
            const otherRabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName } });
            await otherRabbit.enqueueTask(fullTaskName, {}, enqueueOptions);
            await new Promise(resolve => setTimeout(resolve, 100));
            (0, chai_1.expect)(consumeCount).to.equal(0);
            let consumeCountNewConsumer = 0;
            await otherRabbit.registerTaskConsumer(taskName, () => {
                consumeCountNewConsumer++;
            }, myConsumeOptions);
            await new Promise(resolve => setTimeout(resolve, 500));
            (0, chai_1.expect)(consumeCount).to.equal(0);
            (0, chai_1.expect)(consumeCountNewConsumer).to.equal(1);
            await otherRabbit.shutdown();
        });
    });
});
