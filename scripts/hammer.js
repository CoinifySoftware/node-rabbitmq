#!/usr/bin/env node

const cli = require('cli'),
  _ = require('lodash'),
  consoleLogLevel = require('console-log-level');

const CoinifyRabbit = require('../index');

async function run() {
  const options = cli.parse({
    type: ['t', 'Message type, must be either "task" or "event"', 'string'],
    number: ['n', 'Number of messages to publish', 'number', 10],
    name: ['m', 'Name of task or event', 'string'],
    contextString: ['c', 'Context of message, as a JSON-encoded string', 'string', '{}']
  });

  const {type, number, name, contextString} = options;

  if (!type || !_.includes(['task', 'event'], type)) {
    cli.fatal(`Please specify either 'task' or 'event' as Message type (got '${type}')`);
  }

  if (number <= 0) {
    cli.fatal(`Number of messages must be positive (got: ${n})`);
  }

  if (!name) {
    cli.fatal(`Please enter a valid message name (got: ${name})`);
  }

  let context;
  try {
    context = JSON.parse(contextString);
  } catch (err) {
    cli.fatal(`Please input a vaild JSON-encoded string as context (got: '${contextString}'`);
  }

  if (!_.isObject(context)) {
    cli.fatal('Please input an object as context');
  }

  const rabbitOptions = {
    logger: consoleLogLevel({level: 'error'})
  };
  const rabbit = new CoinifyRabbit(rabbitOptions);

  await Promise.all(new Array(number).fill().map(async () => {
    return type === 'event' ? rabbit.emitEvent(name, context, {service: {name: null}}) : rabbit.enqueueTask(name, context);
  }));

  await rabbit.shutdown();

  cli.info(`Published ${number} ${type}s`);
}

async function main() {
  try {
    await run();
  } catch (err) {
    cli.fatal(err);
  }
}

main();
