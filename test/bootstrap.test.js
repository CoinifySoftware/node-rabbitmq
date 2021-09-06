/*
 * Set up chai
 */
const chai = require('chai');
const CoinifyRabbit = require('../lib/CoinifyRabbit');
chai.use(require('chai-as-promised'));
chai.use(require('chai-subset'));

global.expect = chai.expect;
global._ = require('lodash');

function createRabbitMQTestInstance(options = {}) {
  const defaultTestOptions = {
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
  return new CoinifyRabbit({ ...defaultTestOptions, ...options });
}

module.exports = {
  createRabbitMQTestInstance
};
