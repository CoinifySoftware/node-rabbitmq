"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const lodash_1 = __importDefault(require("lodash"));
const bootstrap_test_1 = require("../bootstrap.test");
describe('Integration tests', () => {
    describe('Tasks', () => {
        let taskName, fullTaskName;
        const context = { myContext: false };
        const serviceName = 'my-test-service';
        const enqueueTaskOptions = { exchange: { autoDelete: true } };
        const registerTaskConsumerOptions = { exchange: { autoDelete: true }, queue: { autoDelete: true } };
        let rabbit;
        before(() => {
            rabbit = (0, bootstrap_test_1.createRabbitMQTestInstance)({ service: { name: serviceName } });
        });
        beforeEach(() => {
            taskName = 'my-task' + Math.random();
            fullTaskName = serviceName + '.' + taskName;
        });
        after(async () => {
            await rabbit.shutdown();
        });
        it('should be able to enqueue and consume a single task with a single consumer', () => {
            return new Promise(async (resolve) => {
                await rabbit.registerTaskConsumer(taskName, (c, t) => {
                    (0, chai_1.expect)(t.taskName).to.equal(fullTaskName);
                    (0, chai_1.expect)(c).to.deep.equal(context);
                    resolve(undefined);
                }, registerTaskConsumerOptions);
                await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
            });
        });
        it('should be able to enqueue a task with a pre-defined UUID and timestamp', () => {
            const uuid = '12341234-1234-1234-1234-123412341234';
            const time = 1511944077916;
            const enqueueOptions = lodash_1.default.defaults({ uuid, time }, enqueueTaskOptions);
            return new Promise(async (resolve) => {
                await rabbit.registerTaskConsumer(taskName, (c, t) => {
                    (0, chai_1.expect)(t.uuid).to.equal(uuid);
                    (0, chai_1.expect)(t.time).to.equal(time);
                    resolve(undefined);
                }, registerTaskConsumerOptions);
                await rabbit.enqueueTask(fullTaskName, context, enqueueOptions);
            });
        });
        it('should be able to enqueue and consume multiple tasks with a single consumer', () => {
            const taskCount = 3;
            return new Promise(async (resolve) => {
                const contexts = lodash_1.default.map(lodash_1.default.range(taskCount), i => {
                    return { taskNumber: i };
                });
                let tasksConsumed = 0;
                await rabbit.registerTaskConsumer(taskName, (c, t) => {
                    (0, chai_1.expect)(t.taskName).to.equal(fullTaskName);
                    (0, chai_1.expect)(c).to.deep.equal(contexts[tasksConsumed]);
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
            const consumerIds = lodash_1.default.range(consumerCount);
            const tasksConsumed = [];
            const tasksConsumedByConsumer = {};
            const taskIds = lodash_1.default.range(taskCount);
            const contexts = lodash_1.default.map(taskIds, i => {
                return { taskNumber: i };
            });
            return new Promise(async (resolve) => {
                for (const i of consumerIds) {
                    await rabbit.registerTaskConsumer(taskName, (c, t) => {
                        (0, chai_1.expect)(t.taskName).to.equal(fullTaskName);
                        tasksConsumed.push(c.taskNumber);
                        tasksConsumedByConsumer[i] = (tasksConsumedByConsumer[i] || 0) + 1;
                        if (tasksConsumed.length === taskCount) {
                            const tasksConsumedSorted = lodash_1.default.sortBy(tasksConsumed);
                            (0, chai_1.expect)(tasksConsumedSorted).to.deep.equal(taskIds);
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
            const consumerIds = lodash_1.default.range(consumerCount);
            let tasksConsumed = 0;
            const tasksConsumedByConsumer = {};
            return new Promise(async (resolve) => {
                for (const i of consumerIds) {
                    const consumeOptions = lodash_1.default.defaultsDeep({}, registerTaskConsumerOptions, { uniqueQueue: true });
                    await rabbit.registerTaskConsumer(taskName, (c, e) => {
                        (0, chai_1.expect)(e.taskName).to.equal(fullTaskName);
                        tasksConsumed++;
                        if (!tasksConsumedByConsumer[i]) {
                            tasksConsumedByConsumer[i] = 0;
                        }
                        tasksConsumedByConsumer[i]++;
                        if (tasksConsumed === consumerCount) {
                            (0, chai_1.expect)(lodash_1.default.size(tasksConsumedByConsumer)).to.equal(consumerCount);
                            lodash_1.default.forOwn(tasksConsumedByConsumer, tasksConsumed => {
                                (0, chai_1.expect)(tasksConsumed).to.equal(1);
                            });
                            resolve(undefined);
                        }
                    }, consumeOptions);
                }
                await rabbit.enqueueTask(fullTaskName, { theContext: true }, enqueueTaskOptions);
            });
        });
        it('should retry a task whose processing function rejected', () => {
            return new Promise(async (resolve) => {
                const delayMillis = 250;
                const maxAttempts = 4;
                const consumeOptions = lodash_1.default.defaultsDeep({ retry: { backoff: { type: 'fixed', delay: delayMillis / 1000 }, maxAttempts } }, registerTaskConsumerOptions);
                let startTime = Date.now();
                let attempt = 0;
                await rabbit.registerTaskConsumer(taskName, (c, e) => {
                    (0, chai_1.expect)(e.taskName).to.equal(fullTaskName);
                    (0, chai_1.expect)(e.attempts).to.equal(attempt);
                    (0, chai_1.expect)(c).to.deep.equal(context);
                    const endTime = Date.now();
                    const elapsed = endTime - startTime;
                    startTime = endTime;
                    if (attempt > 0) {
                        (0, chai_1.expect)(elapsed).to.be.at.least(delayMillis);
                        (0, chai_1.expect)(elapsed).to.be.at.most(delayMillis + 200);
                    }
                    attempt++;
                    if (attempt <= maxAttempts) {
                        throw new Error('Processing function rejected');
                    }
                    else {
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
            const consumerOptions = lodash_1.default.defaultsDeep({ consumer: { prefetch } }, registerTaskConsumerOptions);
            const consumeTimestamps = [];
            await new Promise(async (resolve) => {
                await rabbit.registerTaskConsumer(taskName, async () => {
                    await new Promise(resolve => setTimeout(resolve, consumeTime));
                    consumeTimestamps.push(Date.now());
                    if (consumeTimestamps.length === taskCount) {
                        resolve(undefined);
                    }
                }, consumerOptions);
                await Promise.all(new Array(taskCount).fill(undefined).map(() => rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions)));
            });
            const timestampMeans = lodash_1.default.chunk(consumeTimestamps, prefetch).map(lodash_1.default.mean);
            for (let i = 1; i < timestampMeans.length; i++) {
                const diff = timestampMeans[i] - timestampMeans[i - 1];
                (0, chai_1.expect)(diff).to.be.at.least(consumeTime - 100).and.at.most(consumeTime + 100);
            }
        });
        it('should be able to enqueue a delayed task', () => {
            const delayMillis = 1000;
            const enqueueOptions = Object.assign({ delayMillis }, enqueueTaskOptions);
            let enqueueTime;
            return new Promise(async (resolve) => {
                await rabbit.registerTaskConsumer(taskName, (c, t) => {
                    const consumeTime = Date.now();
                    (0, chai_1.expect)(t.taskName).to.equal(fullTaskName);
                    (0, chai_1.expect)(t.delayMillis).to.equal(delayMillis);
                    (0, chai_1.expect)(c).to.deep.equal(context);
                    (0, chai_1.expect)(consumeTime - enqueueTime).to.be.closeTo(delayMillis, 50);
                    resolve(undefined);
                }, registerTaskConsumerOptions);
                enqueueTime = Date.now();
                await rabbit.enqueueTask(fullTaskName, context, enqueueOptions);
            });
        });
        it('should use custom onError function if task consumption rejects', async () => {
            const err = new Error('The error');
            const context = { theContext: true };
            await (0, bootstrap_test_1.disableFailedMessageQueue)(rabbit);
            await new Promise(async (resolve, reject) => {
                await rabbit.registerTaskConsumer(taskName, () => {
                    throw err;
                }, {
                    ...registerTaskConsumerOptions,
                    onError: params => {
                        try {
                            (0, chai_1.expect)(params).to.containSubset({
                                err,
                                context,
                                task: {
                                    taskName: fullTaskName,
                                    context
                                }
                            });
                            resolve(undefined);
                        }
                        catch (err) {
                            reject(err);
                        }
                    }
                });
                await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
            });
            await new Promise(resolve => setTimeout(resolve, 25));
            await (0, bootstrap_test_1.reenableFailedMessageQueue)(rabbit);
        });
        it('should use custom onError function if task consumption throws', async () => {
            const err = new Error('The error');
            const context = { theContext: true };
            await (0, bootstrap_test_1.disableFailedMessageQueue)(rabbit);
            await new Promise(async (resolve, reject) => {
                await rabbit.registerTaskConsumer(taskName, () => {
                    throw err;
                }, {
                    ...registerTaskConsumerOptions,
                    onError: params => {
                        try {
                            (0, chai_1.expect)(params).to.containSubset({
                                err,
                                context,
                                task: {
                                    taskName: fullTaskName,
                                    context
                                }
                            });
                            resolve(undefined);
                        }
                        catch (err) {
                            reject(err);
                        }
                    }
                });
                await rabbit.enqueueTask(fullTaskName, context, enqueueTaskOptions);
            });
            await new Promise(resolve => setTimeout(resolve, 25));
            await (0, bootstrap_test_1.reenableFailedMessageQueue)(rabbit);
        });
    });
});
