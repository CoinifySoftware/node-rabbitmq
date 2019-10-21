# node-rabbitmq 
[![npm version](https://badge.fury.io/js/%40coinify%2Frabbitmq.svg)](https://badge.fury.io/js/%40coinify%2Frabbitmq)

## Suggestions for improvement
* Ping functionality for service health check
* CLI scripts to emit events / enqueue tasks
* Split main lib code into smaller files


## Implementation details
* Everything happens on the same TCP connection to RabbitMQ. It is created first time it is needed, and cached for subsequent requests.
* Everything happens on the same channel inside the TCP connection. It is created first time it is needed, and cached for subsequent requests.

## Connection/channel failure
If the channel or connection closes unexpectedly (i.e. `#close()` was not called), the library will attempt to reconnect
and re-attach the existing consumers transparently.

Re-connection will be attempted using fibonacci backoff with a maximum delay of 1 minute, meaning that the following delays (seconds)
will be used between re-connection failures: 1, 1, 2, 3, 5, 10, 20, 30, 50, 60, 60, 60...

## `CoinifyRabbit` API
```js
const CoinifyRabbit = require('@coinify/rabbitmq');
```

### `new CoinifyRabbit(options)`
_Creates a new instance of `CoinifyRabbit`_

The `options` argument overrides default configuration options that are specified in `config/default.js` and `config/<environment>.js` files
(where `<environment>` is determined by the `NODE_ENV` environment variable).

```js
const options = {
  logger: null, // Bunyan-compatible logger
  service: { // Service-specific options
    name: 'my-service' // Different service names consume events from different queues
  }
  // For all configuration options, see config/default.js
};

const coinifyRabbit = new CoinifyRabbit(options);
```

### `#shutdown(timeout=null): Promise<void>`
_Perform graceful shutdown, optionally with a timeout._

Calling this function closes the active channel and connection, if they are open. It also stops consuming messages
and waits for active consumer functions to finish within the given timeout.

The `timeout` argument specifies the number of _milliseconds_ to wait for all consumer functions to finish.
If one or more active consumer function haven't finished in this time, they are `nack`'ed and returned to the queue
where they came from.

If `timeout = null`, there is no timeout and `shutdown()` will wait for all consumer functions to finish.

The promise resolves when connection and channel is closed, and all consumer functions are have finished _or_ the timeout
has been reached.

## Events

### Topology

All events are published to the **topic** exchange with the name defined in `options.exchanges.eventsTopic`.
The name defaults to `'events.topic'`, and shouldn't really be changed unless you know what you're doing.

Emitting an event (using `#emitEvent(eventName, context)`) publishes a message to this exchange, with the _routing key_
`config.service.name + '.' + eventName`.

Subscribing to consuming an event (using `#registerEventConsumer(eventKey, options, consumeFn)`) binds a queue to this exchange,
using `eventKey` as the _binding key_.
The queue _name_ is determined by the `(eventKey, config.service.name)` pair, meaning that if multiple consumers
call the `registerEventConsumer` function with the same `eventKey` and having the same `config.service.name`, they will consume
from the _same_ queue.
Specifically, the queue name is `'events.' + options.service.name + '.' + eventKey`.

### Event message

Emitting an event with name `eventName` and context `context` publishes the following JSON object:
```js
{
  eventName: fullEventName, // serviceName + '.' + eventName
  context: context,
  uuid: 'd51bbaed-1ee8-4bb6-a739-cee5b56ee518', // Actual UUID generated upon emit
  time: 1504865878534 // Timestamp of event, in milliseconds since UNIX epoc
}
```

### Example

```js
const CoinifyRabbit = require('@coinify/rabbitmq');

const coinifyRabbit = new CoinifyRabbit({service: {name: 'my-service'}});

async function testEvent() {
    await coinifyRabbit.registerEventConsumer('my-service.my-event', async (context, event) => {
      const {eventName, uuid, time} = event;
      console.log('Event consumed', {context, eventName, uuid, time});
      process.exit(0);
    });

    await coinifyRabbit.emitEvent('my-event', {myContext: true});
}

testEvent();
```

### `#emitEvent(eventName, context, options={}): Promise<{eventName, context, uuid, time}>`
_Emits an event with an associated context_

```js
const result = await coinifyRabbit.emitEvent('my-event', {myContext: true});

// result is an object with the below keys
const {eventName, context, uuid, time} = result;
```

**NOTE:** If `options.service.name` is defined, the actual emitted `eventName` is prefixed with the service name and a dot (`.`).

### `#registerEventConsumer(eventKey, consumeFn, options = {}): Promise<string>`
_Registers a function for consuming a specific event_

```js
const consumerTag = await coinifyRabbit.registerEventConsumer('my-service.my-event', async(context, event) => {
  // Resolve to ACK event, removing it from the queue
  // Reject and event will not be ACK'ed

  // context is the same as emitted context object
  // event is object of {eventName, uuid, time}
  console.log({context, event});
});

// consumerTag is used to cancel consumer again at a later point. (Not yet implemented)
```

The following properties can be set in  `options`:

* `retry`: Configure retry mechanism for this consumer. See _Retry_ section for more information
* `uniqueQueue`: Create a unique queue (`true`) instead of default
behaviour (`false`) where each instance of the service consumes from the same queue.
Setting this to `true` will cause the event to be consumed by _each_ event consumer.
* `consumer`: Consumer-specific options. Must be an object with the following properties:
  * `prefetch`: Prefetch value for this consumer. See _Prefetch_ section for more information.
* `service`: Service-specific options. Must be an object with the following properties:
  * `name`: Overrides the `options.service.name` set in the `CoinifyRabbit` constructor.
    Prefixes the consumed task name with `name` and a dot (`.`).

#### `eventKey` wildcards
Given that events are emitted to a RabbitMQ _topic_ exchange,
you can use wildcards in the `eventKey` argument to consume more than one event.
Dots (`.`) are used to separate _words_, which can be replaced with wildcards.

* `*` can substitute for _exactly_ one word
* `#` can substitute for zero or more words

##### Examples
Imagine that four consumers are registered, using the following `eventKey`s:

* (a) `my-service.my-event.happened` (specific event, no wildcards)
* (b) `my-service.my-event.*`
* (c) `my-service.#`
* (d) `#`

Then, four events with the following event names are emitted:

* `my-service.my-event.happened`
  * Consumed by all 4 consumers (a+b+c+d)
* `my-service.my-event.failed`
  * Consumed by 3 consumers (b+c+d)
* `my-service.another-event.happened`
  * Consumed by 2 consumers (c+d)
* `yourService.shit.hit.the.fan`
  * Consumed only by consumer (d)


## Tasks

### Topology

All tasks are published to the **topic** exchange with the name defined in `options.exchanges.tasksTopic`.
The name defaults to `'tasks.topic'`, and shouldn't really be changed unless you know what you're doing.

Enqueueing a task (using `#enqueueTask(taskName, context)`) publishes a message to this exchange, with the _routing key_
`taskName`.

Subscribing to consuming a task (using `#registerTaskConsumer(taskName, consumeFn)`) creates a queue with the name
`'tasks.' + options.service.name + '.' + taskName`, and binds it to the exchange,
using `options.service.name + '.' + taskName` as the _binding key_.

### Task message

Enqueueing a task with name `taskName` and context `context` publishes the following JSON object:
```js
{
  taskName: taskName,
  context: context,
  uuid: 'f07d33d7-f56f-4c89-9489-1bc89d3a6483', // Actual UUID generated upon emit
  time: 1506331856322, // Timestamp of event, in milliseconds since UNIX epoc
  attempt: 0, // number of current attempt. Used by retry mechanism
  origin: 'another-service' // Name of service that enqueued the task
}
```

### Example

```js
const CoinifyRabbit = require('@coinify/rabbitmq');

const coinifyRabbit = new CoinifyRabbit({service: {name: 'my-service'}});

async function testTask() {
    await rabbit.registerTaskConsumer('my-task', async (context, task) => {
      const {taskName, uuid, time} = task;
      console.log('Task consumed', {context, taskName, uuid, time});
      process.exit(0);
    });
    await rabbit.enqueueTask('my-service.my-task', context);
}

testTask();
```

### `#enqueueTask(taskName, context, options={}): Promise<{taskName, context, uuid, time}>`
_Enqueues a task with an associated context_

```js
const result = await coinifyRabbit.enqueueTask('my-service.my-task', {myContext: true});

// result is true if task was enqueued correctly.
```

### `#registerTaskConsumer(taskName, consumeFn, options={})`
_Registers a function for consuming a specific task_

```js
const consumeOptions = {
  retry: {
    backoff: {
      type: 'exponential'
    },
    // Retry at most two times (initial attempt, first retry, second retry)
    maxAttempts: 2
  }
};

const consumerTag = await coinifyRabbit.registerTaskConsumer('my-task', async(context, task) => {
  // Resolve to ACK task, removing it from the queue
  // Reject and task will not be ACK'ed

  // context is the same as enqueued context object
  // task is object of {taskName, uuid, time}
  console.log({context, task});
}, consumeOptions);

// consumerTag is used to cancel consumer again at a later point. (Not yet implemented)
```

The given `taskName` will be prefixed with `'<serviceName>.'` to produce a full task name, which is used for consumption.
`<serviceName>` is defined in `options.service.name` in `CoinifyRabbit` constructor or directly in `options` object.

The following properties can be set in  `options`:

* `retry`: Configure retry mechanism for this consumer. See _Retry_ section for more information
* `uniqueQueue`: Create a unique queue (`true`) instead of default
behaviour (`false`) where each instance of the service consumes from the same queue.
Setting this to `true` will cause the event to be consumed by _each_ event consumer.
* `consumer`: Consumer-specific options. Must be an object with the following properties:
  * `prefetch`: Prefetch value for this consumer. See _Prefetch_ section for more information.
* `service`: Service-specific options. Must be an object with the following properties:
  * `name`: Overrides the `options.service.name` set in the `CoinifyRabbit` constructor.
    Prefixes the consumed task name with `name` and a dot (`.`).

## Retry
Retry functionality for tasks and events is implemented using (Dead Letter Exchanges)[https://www.rabbitmq.com/dlx.html] and
(Per-Queue Message TTL)[https://www.rabbitmq.com/ttl.html]:

When consumption of a message fails, a retry mechanism (see `options.retry` argument to `#registerTaskConsumer()`) determines
whether the message should be retried at a later point in time, and if so, how much time to wait (the _delay_) until the next attempt.

If the message should _not_ be retried (i.e. consumer is configured to not retry, or maximum number of retry attempts reached)
the message will be republished to the **fanout** exchange with the name defined in `options.exchanges.failed`,
which defaults to `'_failed'`. To this exchange is bound a single queue with the name defined in `options.queues.failed`,
which also defaults to `'_failed'`. **Failed messages will remain here until manually removed**.

If the message should be retried with a _delay_  of `t` milliseconds, it will be re-published to the **direct** exchange with the name
`options.exchanges.retry`.
To this exchange is bound queues with the names `options.queues.retryPrefix + '.' + t + 'ms'`, which are bound to the exchange.
The queues are configured to have messages expire after `t` milliseconds, and republished back to the default **direct**,
using the name of the original queue as the routing key.

When registering a consumer, the following retry properties can be set in the `options` argument:
* `retry`: Specify how (if at all) performing the task should be retried on failure (if `consumeFn` rejects).
  If not specified, defaults no retry (`false`).
  If specified, must be an `false` or an object with the following properties:
    * `backoff`: Backoff definition. If specified, must be an object with the following properties:
        * `type`: Backoff type: Must be `'exponential'` or `'fixed'`. Defaults to `fixed`
            * For `exponential` backoff, the delay until next retry is calculated as `(delay * (base ^ n))`,
              where `n` is the current attempt (0-indexed). First retry is thus always after `delay` seconds
            * For `fixed` backoff, the delay until next retry is always `delay`
        * `delay`: Delay in seconds for first retry. Defaults to 16
        * `base`: (Only for `exponential` type) The base number for the exponentiation. Defaults to 2
    * `maxAttempts`: The maximum number of retry attempts. Defaults to 12.
      If set to e.g. 1, the task will at most be run twice: One for the original attempt, and one retry attempt.
      Setting this to 0 is the same as setting `retry: false`.

#### `consumeFn` usage

If `consumeFn` rejects with an Error that has `noRetry: true` property set, the task will _not_ be retried
regardless of what the `options.retry` settings specify.


## Prefetch
Prefetch values (limits on the number of unacknowledged messages) can be set both on a per-channel and per-consumer basis.

Per-channel value is set in the configuration value `channel.prefetch`.

Per-consumer value is set by default in the configuration value `consumer.prefetch`, which can be overridden for
individual consumers using the `options.consumer.prefetch` argument to `registerEventConsumer` and `registerTaskConsumer` functions.

**NOTE**: When overriding default per-consumer prefetch value (using `options.consumer.prefetch` argument),
you must take to register consumers _serially_ (i.e. not in parallel) due to possible race conditions mixing
up prefetch values.

See (Consumer prefetch)[https://www.rabbitmq.com/consumer-prefetch.html] and (Confirms)[https://www.rabbitmq.com/confirms.html]
for more information.

## Failed Messages
As described in the section about retry, all failed messages will end up in the `_failed` queue. We can then consume them and manually re-enqueue the messages to a specified queue we want to retry by using the functions below.

### `#registerFailedMessageConsumer(consumeFn, options={})`

_Registers a function for consuming a task or event from the queue of failed messages_
```js
const consumerTag = await coinifyRabbit.registerFailedMessageConsumer(async(routingKey, message) => {
  // Resolve to ACK task, removing it from the queue
  // Reject and task will bet NACK'ed and re-enqueued

  // routingKey is the name of the queue that the message was failed from
  // message is object of {eventName|taskName, context, uuid, time, attempts}
  console.log({context, message});
});
```

`#registerFailedMessageConsumer` bears resemblance to `#registerTaskConsumer` and `#registerEventConsumer`, however it will not need to match to any (event or task) name.

The following properties can be set in `options`:

* `consumer`: Consumer-specific options. Must be an object with the following properties:
  * `prefetch`: Prefetch value for this consumer. See _Prefetch_ section for more information.

### `#enqueueMessage(queueName, messageObject)`

_Enqueues a message to a specific queue. This can be of type [event](#event-message) or [task](#task-message) message_

```js
const messageObject = {
  eventName: fullEventName, // serviceName + '.' + eventName
  context: context,
  uuid: 'd51bbaed-1ee8-4bb6-a739-cee5b56ee518', // Actual UUID generated upon emit
  time: 1504865878534 // Timestamp of event, in milliseconds since UNIX epoc
}

const result = await coinifyRabbit.enqueueMessage('events.accounting.trade.trade-completed', messageObject);

// result is true if message was enqueued correctly.
```

### Failed Message Handling

It is possible to setup and use `#registerFailedMessageConsumer` and `#enqueueMessage` for handling failed message and evaluating them for re-enqueueing:

```js
const consumerTag = await coinifyRabbit.registerFailedMessageConsumer(async(routingKey, message) => {
  // Logic for determining whether the failed message
  // should be re-enqueued or if other action should be taken.

  // If it wishes to re-enqueue do:
  const result = await coinifyRabbit.enqueueMessage(routingKey, message);
});
```

Alternatively, if one is in a scenario where all failed messages should be re-enqueued right away, it can be done as:

```js
const consumerTag = await coinifyRabbit.registerFailedMessageConsumer(async (routingKey, message) => coinifyRabbit.enqueueMessage(routingKey, message));
```
