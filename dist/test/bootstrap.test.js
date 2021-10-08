"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.reenableFailedMessageQueue = exports.disableFailedMessageQueue = exports.createRabbitMQTestInstance = void 0;
const sinon_1 = __importDefault(require("sinon"));
const chai_1 = __importDefault(require("chai"));
const chai_as_promised_1 = __importDefault(require("chai-as-promised"));
const chai_subset_1 = __importDefault(require("chai-subset"));
const lodash_defaultsdeep_1 = __importDefault(require("lodash.defaultsdeep"));
require("mocha");
const CoinifyRabbit_1 = __importDefault(require("../src/CoinifyRabbit"));
chai_1.default.use(chai_as_promised_1.default);
chai_1.default.use(chai_subset_1.default);
function createRabbitMQTestInstance(options) {
    const defaultTestOptions = {
        defaultLogLevel: 'fatal',
        exchanges: {
            retry: 'test._retry',
            tasksTopic: 'test.tasks.topic',
            delayed: 'test._delayed',
            failed: 'test._failed',
            eventsTopic: 'test.events.topic'
        },
        queues: {
            retryPrefix: 'test._retry',
            delayedTaskPrefix: 'test._delay.tasks',
            failed: 'test._failed'
        }
    };
    return new CoinifyRabbit_1.default((0, lodash_defaultsdeep_1.default)(defaultTestOptions, options));
}
exports.createRabbitMQTestInstance = createRabbitMQTestInstance;
async function disableFailedMessageQueue(rabbit) {
    const channel = await rabbit._getChannel();
    const publishStub = sinon_1.default.stub(channel, 'publish');
    const config = rabbit.config;
    publishStub.withArgs(config.exchanges.failed, sinon_1.default.match.any, sinon_1.default.match.any, sinon_1.default.match.any).returns(true);
    publishStub.callThrough();
}
exports.disableFailedMessageQueue = disableFailedMessageQueue;
async function reenableFailedMessageQueue(rabbit) {
    const channel = await rabbit._getChannel();
    const publishStub = channel.publish;
    publishStub.restore();
}
exports.reenableFailedMessageQueue = reenableFailedMessageQueue;
process.on('unhandledRejection', (err) => {
    console.error('+++++ UNHANDLED REJECTION +++++');
    console.error(err);
    process.exit(1);
});
process.on('uncaughtException', (err) => {
    console.error('+++++ UNCAUGHT EXCEPTION +++++');
    console.error(err);
    process.exit(1);
});
