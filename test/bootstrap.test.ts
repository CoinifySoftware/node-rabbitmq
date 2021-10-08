/* eslint-disable no-console */
import sinon from 'sinon';
import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';
import chaiSubset from 'chai-subset';
import defaultsDeep from 'lodash.defaultsdeep';
import 'mocha';
import CoinifyRabbit, { CoinifyRabbitConstructorOptions } from '../src/CoinifyRabbit';
import CoinifyRabbitConfiguration from '../src/CoinifyRabbitConfiguration';
import DeepPartial from '../src/DeepPartial';

/*
 * Set up chai
 */
chai.use(chaiAsPromised);
chai.use(chaiSubset);

export function createRabbitMQTestInstance(options?: CoinifyRabbitConstructorOptions): CoinifyRabbit {
  const defaultTestOptions: DeepPartial<CoinifyRabbitConfiguration> = {
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
  return new CoinifyRabbit(defaultsDeep(defaultTestOptions, options ));
}

export async function disableFailedMessageQueue(rabbit: CoinifyRabbit) {
  const channel = await rabbit._getChannel();
  const publishStub = sinon.stub(channel, 'publish');
  const config = (rabbit as any).config as CoinifyRabbitConfiguration;
  publishStub.withArgs(config.exchanges.failed, sinon.match.any, sinon.match.any, sinon.match.any).returns(true);
  publishStub.callThrough();
}

export async function reenableFailedMessageQueue(rabbit: CoinifyRabbit) {
  const channel = await rabbit._getChannel();
  const publishStub = channel.publish as sinon.SinonStub;
  publishStub.restore();
}

process.on('unhandledRejection', (err: any) => {
  console.error('+++++ UNHANDLED REJECTION +++++');
  console.error(err);
  process.exit(1);
});
process.on('uncaughtException', (err) => {
  console.error('+++++ UNCAUGHT EXCEPTION +++++');
  console.error(err);
  process.exit(1);
});
