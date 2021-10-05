import CoinifyRabbit, { CoinifyRabbitConstructorOptions } from './src/CoinifyRabbit';
import CoinifyRabbitConfiguration, { CoinifyRabbitConnectionConfiguration } from './src/CoinifyRabbitConfiguration';
import Logger from './src/interfaces/Logger';
import Event, {
  EventConsumerFunction,
  OnEventErrorFunctionParams,
  RegisterEventConsumerOptions
} from './src/messageTypes/Event';
import {
  FailedMessage,
  FailedMessageConsumerFunction,
  FailedMessageConsumer,
  RegisterFailedMessageConsumerOptions
} from './src/messageTypes/FailedMessage';
import Task, {
  TaskConsumerFunction,
  OnTaskErrorFunctionParams,
  RegisterTaskConsumerOptions
} from './src/messageTypes/Task';

export default CoinifyRabbit;
export { CoinifyRabbitConstructorOptions };

export { CoinifyRabbitConfiguration, CoinifyRabbitConnectionConfiguration };
export { Logger };

export { Event, EventConsumerFunction, OnEventErrorFunctionParams, RegisterEventConsumerOptions };
export { Task, TaskConsumerFunction, OnTaskErrorFunctionParams, RegisterTaskConsumerOptions };
export { FailedMessage, FailedMessageConsumerFunction, FailedMessageConsumer, RegisterFailedMessageConsumerOptions };
