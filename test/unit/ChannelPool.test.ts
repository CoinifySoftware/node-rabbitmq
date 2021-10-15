import amqplib from 'amqplib';
import { expect } from 'chai';
import EventEmitter from 'events';
import sinon from 'sinon';
import ChannelPool from '../../src/ChannelPool';
import consoleLogLevel from 'console-log-level';

describe('ChannelPool', () => {

  let _getConnectionStub: sinon.SinonStub,
    _onChannelOpenedStub: sinon.SinonStub,
    _onChannelClosedStub: sinon.SinonStub,
    createChannelStub: sinon.SinonStub,
    createConfirmChannelStub: sinon.SinonStub,
    amqpConnection: amqplib.Connection,
    pool: ChannelPool;

  beforeEach(() => {
    createChannelStub = sinon.stub();
    createConfirmChannelStub = sinon.stub();
    amqpConnection = {
      createChannel: createChannelStub,
      createConfirmChannel: createConfirmChannelStub
    } as any;

    _getConnectionStub = sinon.stub();
    _getConnectionStub.resolves(amqpConnection);
    _onChannelOpenedStub = sinon.stub();
    _onChannelOpenedStub.resolves();
    _onChannelClosedStub = sinon.stub();
    _onChannelClosedStub.resolves();

    pool = new ChannelPool(
      consoleLogLevel({ level: 'fatal' }),
      _getConnectionStub,
      _onChannelOpenedStub,
      _onChannelClosedStub
    );
  });

  it('should create and cache different types of channels', async () => {
    const consumerChannel = new EventEmitter();
    const publisherChannel = new EventEmitter();
    const publisherConfirmChannel = new EventEmitter();

    createChannelStub.onCall(0).resolves(consumerChannel);
    createChannelStub.onCall(1).resolves(publisherChannel);
    createChannelStub.rejects(new Error('amqplib.createChannel() called more than twice.'));

    createConfirmChannelStub.onCall(0).resolves(publisherConfirmChannel);
    createConfirmChannelStub.rejects(new Error('amqplib.createConfirmChannelStub() called more than once.'));

    // First call to getConsumerChannel, should call amqplib.createChannel()
    expect(await pool.getConsumerChannel()).to.equal(consumerChannel);
    expect(createChannelStub.callCount).to.equal(1);

    // Second call to getConsumerChannel, should use cached connection
    expect(await pool.getConsumerChannel()).to.equal(consumerChannel);
    expect(createChannelStub.callCount).to.equal(1);

    // First call to getPublisherChannel with confirm: false, should call amqplib.createChannel()
    expect(await pool.getPublisherChannel(false)).to.equal(publisherChannel);
    expect(createChannelStub.callCount).to.equal(2);

    // Second call to getPublisherChannel with confirm: false, should use cached connection
    expect(await pool.getPublisherChannel(false)).to.equal(publisherChannel);
    expect(createChannelStub.callCount).to.equal(2);


    expect(createConfirmChannelStub.callCount).to.equal(0);

    // First call to getPublisherChannel with confirm: true, should call amqplib.createConfirmChannel()
    expect(await pool.getPublisherChannel(true)).to.equal(publisherConfirmChannel);
    expect(createConfirmChannelStub.callCount).to.equal(1);

    // Second call to getPublisherChannel with confirm: false, should use cached connection
    expect(await pool.getPublisherChannel(true)).to.equal(publisherConfirmChannel);
    expect(createConfirmChannelStub.callCount).to.equal(1);

    expect(_onChannelOpenedStub.callCount).to.equal(3);
    expect(_onChannelOpenedStub.firstCall.args[0]).to.equal( consumerChannel );
    expect(_onChannelOpenedStub.secondCall.args[0]).to.equal( publisherChannel );
    expect(_onChannelOpenedStub.thirdCall.args[0]).to.equal( publisherConfirmChannel );
  });

  it('should create and cache a single connection, even if creating the connection is not instantaneous', async () => {
    const channel = new EventEmitter();
    createChannelStub.onCall(0).resolves(channel);
    createChannelStub.rejects(new Error('amqplib.createChannel() called more than once.'));

    // Get three connections in parallel
    const [ conn1, conn2, conn3 ] = await Promise.all([ pool.getConsumerChannel(), pool.getConsumerChannel(), pool.getConsumerChannel() ]);

    expect(conn1).to.equal(channel);
    expect(conn2).to.equal(channel);
    expect(conn3).to.equal(channel);

    expect(createChannelStub.calledOnce).to.equal(true);

    // Try again with three more connections, expect the caching to have already happened
    const [ conn4, conn5, conn6 ] = await Promise.all([ pool.getConsumerChannel(), pool.getConsumerChannel(), pool.getConsumerChannel() ]);

    expect(conn4).to.equal(channel);
    expect(conn5).to.equal(channel);
    expect(conn6).to.equal(channel);

    expect(createChannelStub.calledOnce).to.equal(true);

    expect(_onChannelOpenedStub.calledOnce).to.equal(true);
    expect(_onChannelOpenedStub.firstCall.args[0]).to.equal( channel );
  });

  it('should create a channel, and drop it if an error occurs', async () => {
    const channel = new EventEmitter();

    createChannelStub.resolves(channel);

    // First call, should call amqplib.createChannel()
    expect(await pool.getConsumerChannel()).to.equal(channel);
    expect(createChannelStub.calledOnce).to.equal(true);

    // Emit an error, expecting the connection cache to be flushed
    channel.emit('error', new Error('Test channel error'));
    // Wait a bit to ensure that the event is caught
    await new Promise(resolve => setImmediate(resolve));

    // Second call, should call amqplib.createChannel() again
    expect(await pool.getConsumerChannel()).to.equal(channel);
    expect(createChannelStub.calledTwice).to.equal(true);

    // _onChannelOpened() should be called twice, one for each channel opened
    expect(_onChannelOpenedStub.calledTwice).to.equal(true);
    expect(_onChannelOpenedStub.firstCall.args[0]).to.equal( channel );
    expect(_onChannelOpenedStub.secondCall.args[0]).to.equal( channel );
  });

  it('should create a channel, and then delete cached channel and call _onChannelClosed() if channel emits \'close\' event', async () => {
    const channel = new EventEmitter();

    createChannelStub.resolves(channel);

    expect(await pool.getConsumerChannel()).to.equal(channel);

    expect(pool.isChannelOpen('consumer')).to.equal(true);

    // Emit a close event, expecting the connection cache to be flushed
    channel.emit('close');
    // Wait a bit to ensure that the event is caught
    await new Promise(resolve => setImmediate(resolve));

    expect(_onChannelClosedStub.calledOnce).to.equal(true);

    // Cached channel should be gone
    expect(pool.isChannelOpen('consumer')).to.equal(false);
  });

});
