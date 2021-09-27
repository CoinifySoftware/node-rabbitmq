import 'mocha';
import CoinifyRabbit, { CoinifyRabbitConstructorOptions } from '../src/CoinifyRabbit';
export declare function createRabbitMQTestInstance(options?: CoinifyRabbitConstructorOptions): CoinifyRabbit;
export declare function disableFailedMessageQueue(rabbit: CoinifyRabbit): Promise<void>;
export declare function reenableFailedMessageQueue(rabbit: CoinifyRabbit): Promise<void>;
