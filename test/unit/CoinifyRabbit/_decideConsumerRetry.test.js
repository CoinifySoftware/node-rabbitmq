'use strict';

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

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
      const { shouldRetry } = CoinifyRabbit._decideConsumerRetry(0, false);
      expect(shouldRetry).to.equal(false);
    });

    it('should not retry if options is null', () => {
      const { shouldRetry } = CoinifyRabbit._decideConsumerRetry(0, null);
      expect(shouldRetry).to.equal(false);
    });

    it('should not retry if options is undefined', () => {
      const { shouldRetry } = CoinifyRabbit._decideConsumerRetry(0);
      expect(shouldRetry).to.equal(false);
    });

    it('should not retry if maxAttempts exceeded', () => {
      /*
       * Default maxAttempts (12)
       */
      let shouldRetry = CoinifyRabbit._decideConsumerRetry(11, fixedOptions).shouldRetry;
      expect(shouldRetry).to.equal(true);

      shouldRetry = CoinifyRabbit._decideConsumerRetry(12, fixedOptions).shouldRetry;
      expect(shouldRetry).to.equal(false);

      /*
       * Specific maxAttempts (3)
       */
      shouldRetry = CoinifyRabbit._decideConsumerRetry(2, { maxAttempts: 3 }).shouldRetry;
      expect(shouldRetry).to.equal(true);

      shouldRetry = CoinifyRabbit._decideConsumerRetry(3, { maxAttempts: 3 }).shouldRetry;
      expect(shouldRetry).to.equal(false);
    });

    it('should compute fixed retry up using default delay until maxAttempts', () => {
      /*
       * Default delay (16 seconds)
       */
      let attempt = 0;
      do {
        const { shouldRetry, delaySeconds } = CoinifyRabbit._decideConsumerRetry(attempt, fixedOptions);
        expect(shouldRetry).to.equal(true);
        expect(delaySeconds).to.equal(16);

        attempt++;
      }
      while (attempt < 12);

      // Try one last time, expecting no more retry
      const retryResult = CoinifyRabbit._decideConsumerRetry(attempt, fixedOptions);
      expect(retryResult.shouldRetry).to.equal(false);
    });

    it('should support infinite retry using fixed backoff with maxAttempts=-1', () => {
      /*
       * Default delay (16 seconds)
       */
      const specificDelayOptions = _.defaultsDeep({ maxAttempts: -1 }, fixedOptions);
      let attempt = 0;
      do {
        const { shouldRetry, delaySeconds } = CoinifyRabbit._decideConsumerRetry(attempt, specificDelayOptions);
        expect(shouldRetry).to.equal(true);
        expect(delaySeconds).to.equal(16);

        attempt++;
      }
      while (attempt < 1000);
    });

    it('should compute fixed retry up using specific delay until maxAttempts', () => {
      /*
       * Specific delay (180 seconds)
       */
      const specificDelayOptions = _.defaultsDeep({ backoff: { delay: 180 } }, fixedOptions);
      let attempt = 0;
      do {
        const { shouldRetry, delaySeconds } = CoinifyRabbit._decideConsumerRetry(attempt, specificDelayOptions);
        expect(shouldRetry).to.equal(true);
        expect(delaySeconds).to.equal(180);

        attempt++;
      }
      while (attempt < 12);

      // Try one last time, expecting no more retry
      const retryResult = CoinifyRabbit._decideConsumerRetry(attempt, specificDelayOptions);
      expect(retryResult.shouldRetry).to.equal(false);
    });

    it('should compute exponential backoff using default base and delay up until maxAttempts', () => {
      /*
       * Default delay is 16 seconds, default base is 2
       */
      const expectedDelays = [ 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768 ];
      let attempt = 0;
      do {
        const { shouldRetry, delaySeconds } = CoinifyRabbit._decideConsumerRetry(attempt, exponentialOptions);
        expect(shouldRetry).to.equal(true);
        expect(delaySeconds).to.equal(expectedDelays[attempt]);

        attempt++;
      }
      while (attempt < 12);

      // Try one last time, expecting no more retry
      const retryResult = CoinifyRabbit._decideConsumerRetry(attempt, exponentialOptions);
      expect(retryResult.shouldRetry).to.equal(false);
    });

    it('should compute exponential backoff using specific base and delay up until maxAttempts', () => {
      /*
       * Specific Delay is 7 seconds, default base is 7, maxAttempts is 7
       */
      const specificOptions = _.defaultsDeep({ backoff: { delay: 7, base: 7 }, maxAttempts: 7 }, exponentialOptions);
      const expectedDelays = [ 7, 49, 343, 2401, 16807, 117649, 823543 ];
      let attempt = 0;
      do {
        const { shouldRetry, delaySeconds } = CoinifyRabbit._decideConsumerRetry(attempt, specificOptions);
        expect(shouldRetry).to.equal(true);
        expect(delaySeconds).to.equal(expectedDelays[attempt]);

        attempt++;
      }
      while (attempt < 7);

      // Try one last time, expecting no more retry
      const retryResult = CoinifyRabbit._decideConsumerRetry(attempt, specificOptions);
      expect(retryResult.shouldRetry).to.equal(false);
    });

    it('should throw an exception for an unknown backoff type', () => {
      const options = { backoff: { type: 'unknown-type' } };
      expect(CoinifyRabbit._decideConsumerRetry.bind(null, 0, options)).to.throw('backoff.type');
    });

    it('should throw an exception for negative maxAttempts (not -1)', () => {
      const options = { maxAttempts: -2 };
      expect(CoinifyRabbit._decideConsumerRetry.bind(null, 0, options)).to.throw('maxAttempts');
    });

    it('should throw an exception for negative backoff.delay', () => {
      const options = { backoff: { delay: -0.5 } };
      expect(CoinifyRabbit._decideConsumerRetry.bind(null, 0, options)).to.throw('backoff.delay');
    });

  });

});