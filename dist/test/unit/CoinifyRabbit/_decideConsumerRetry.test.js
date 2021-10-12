"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_decideConsumerRetry', () => {
        const fixedOptions = {
            backoff: {
                type: 'fixed'
            }
        };
        const exponentialOptions = {
            backoff: {
                type: 'exponential'
            }
        };
        it('should not retry if options is false', () => {
            const { shouldRetry } = CoinifyRabbit_1.default._decideConsumerRetry(0, false);
            (0, chai_1.expect)(shouldRetry).to.equal(false);
        });
        it('should not retry if options is null', () => {
            const { shouldRetry } = CoinifyRabbit_1.default._decideConsumerRetry(0, null);
            (0, chai_1.expect)(shouldRetry).to.equal(false);
        });
        it('should not retry if options is undefined', () => {
            const { shouldRetry } = CoinifyRabbit_1.default._decideConsumerRetry(0);
            (0, chai_1.expect)(shouldRetry).to.equal(false);
        });
        it('should not retry if maxAttempts exceeded', () => {
            let shouldRetry = CoinifyRabbit_1.default._decideConsumerRetry(11, fixedOptions).shouldRetry;
            (0, chai_1.expect)(shouldRetry).to.equal(true);
            shouldRetry = CoinifyRabbit_1.default._decideConsumerRetry(12, fixedOptions).shouldRetry;
            (0, chai_1.expect)(shouldRetry).to.equal(false);
            shouldRetry = CoinifyRabbit_1.default._decideConsumerRetry(2, { maxAttempts: 3 }).shouldRetry;
            (0, chai_1.expect)(shouldRetry).to.equal(true);
            shouldRetry = CoinifyRabbit_1.default._decideConsumerRetry(3, { maxAttempts: 3 }).shouldRetry;
            (0, chai_1.expect)(shouldRetry).to.equal(false);
        });
        it('should compute fixed retry up using default delay until maxAttempts', () => {
            let attempt = 0;
            do {
                const { shouldRetry, delaySeconds } = CoinifyRabbit_1.default._decideConsumerRetry(attempt, fixedOptions);
                (0, chai_1.expect)(shouldRetry).to.equal(true);
                (0, chai_1.expect)(delaySeconds).to.equal(16);
                attempt++;
            } while (attempt < 12);
            const retryResult = CoinifyRabbit_1.default._decideConsumerRetry(attempt, fixedOptions);
            (0, chai_1.expect)(retryResult.shouldRetry).to.equal(false);
        });
        it('should support infinite retry using fixed backoff with maxAttempts=-1', () => {
            const specificDelayOptions = { ...fixedOptions, maxAttempts: -1 };
            let attempt = 0;
            do {
                const { shouldRetry, delaySeconds } = CoinifyRabbit_1.default._decideConsumerRetry(attempt, specificDelayOptions);
                (0, chai_1.expect)(shouldRetry).to.equal(true);
                (0, chai_1.expect)(delaySeconds).to.equal(16);
                attempt++;
            } while (attempt < 1000);
        });
        it('should compute fixed retry up using specific delay until maxAttempts', () => {
            const specificDelayOptions = { ...fixedOptions, backoff: { delay: 180 } };
            let attempt = 0;
            do {
                const { shouldRetry, delaySeconds } = CoinifyRabbit_1.default._decideConsumerRetry(attempt, specificDelayOptions);
                (0, chai_1.expect)(shouldRetry).to.equal(true);
                (0, chai_1.expect)(delaySeconds).to.equal(180);
                attempt++;
            } while (attempt < 12);
            const retryResult = CoinifyRabbit_1.default._decideConsumerRetry(attempt, specificDelayOptions);
            (0, chai_1.expect)(retryResult.shouldRetry).to.equal(false);
        });
        it('should compute exponential backoff using default base and delay up until maxAttempts', () => {
            const expectedDelays = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768];
            let attempt = 0;
            do {
                const { shouldRetry, delaySeconds } = CoinifyRabbit_1.default._decideConsumerRetry(attempt, exponentialOptions);
                (0, chai_1.expect)(shouldRetry).to.equal(true);
                (0, chai_1.expect)(delaySeconds).to.equal(expectedDelays[attempt]);
                attempt++;
            } while (attempt < 12);
            const retryResult = CoinifyRabbit_1.default._decideConsumerRetry(attempt, exponentialOptions);
            (0, chai_1.expect)(retryResult.shouldRetry).to.equal(false);
        });
        it('should compute exponential backoff using specific base and delay up until maxAttempts', () => {
            const specificOptions = { backoff: { type: 'exponential', delay: 7, base: 7 }, maxAttempts: 7 };
            const expectedDelays = [7, 49, 343, 2401, 16807, 117649, 823543];
            let attempt = 0;
            do {
                const { shouldRetry, delaySeconds } = CoinifyRabbit_1.default._decideConsumerRetry(attempt, specificOptions);
                (0, chai_1.expect)(shouldRetry).to.equal(true);
                (0, chai_1.expect)(delaySeconds).to.equal(expectedDelays[attempt]);
                attempt++;
            } while (attempt < 7);
            const retryResult = CoinifyRabbit_1.default._decideConsumerRetry(attempt, specificOptions);
            (0, chai_1.expect)(retryResult.shouldRetry).to.equal(false);
        });
        it('should throw an exception for an unknown backoff type', () => {
            const options = { backoff: { type: 'unknown-type' } };
            (0, chai_1.expect)(CoinifyRabbit_1.default._decideConsumerRetry.bind(null, 0, options)).to.throw('backoff.type');
        });
        it('should throw an exception for negative maxAttempts (not -1)', () => {
            const options = { maxAttempts: -2 };
            (0, chai_1.expect)(CoinifyRabbit_1.default._decideConsumerRetry.bind(null, 0, options)).to.throw('maxAttempts');
        });
        it('should throw an exception for negative backoff.delay', () => {
            const options = { backoff: { delay: -0.5 } };
            (0, chai_1.expect)(CoinifyRabbit_1.default._decideConsumerRetry.bind(null, 0, options)).to.throw('backoff.delay');
        });
    });
});
