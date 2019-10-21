'use strict';

const sinon = require('sinon'),
  rewire = require('rewire'),
  EventEmitter = require('events');

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

const amqplib = require('amqplib');

describe('CoinifyRabbit', () => {

  describe('#_getChannel', () => {

    let _getConnectionStub,
      _onChannelOpenedStub,
      _onChannelClosedStub,
      createChannelStub,
      amqpConnection,
      rabbit;

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      createChannelStub = sinon.stub();
      amqpConnection = { createChannel: createChannelStub };

      _getConnectionStub = sinon.stub(rabbit, '_getConnection');
      _getConnectionStub.resolves(amqpConnection);
      _onChannelOpenedStub = sinon.stub(rabbit, '_onChannelOpened');
      _onChannelOpenedStub.resolves();
      _onChannelClosedStub = sinon.stub(rabbit, '_onChannelClosed');
      _onChannelClosedStub.resolves();
    });

    afterEach(() => {
      _getConnectionStub.restore();
      _onChannelOpenedStub.restore();
      _onChannelClosedStub.restore();
    });

    it('should create and cache a channel', async () => {
      const channel = new EventEmitter();
      createChannelStub.onCall(0).resolves(channel);
      createChannelStub.rejects(new Error('amqplib.createChannel() called more than once.'));

      // First call, should call amqplib.createChannel()
      expect(await rabbit._getChannel()).to.equal(channel);
      expect(createChannelStub.calledOnce).to.equal(true);

      // Second call, should use cached connection
      expect(await rabbit._getChannel()).to.equal(channel);
      expect(createChannelStub.calledOnce).to.equal(true);

      expect(_onChannelOpenedStub.calledOnce).to.equal(true);
      expect(_onChannelOpenedStub.firstCall.args).to.deep.equal([ channel ]);
    });

    it('should create and cache a single connection, even if creating the connection is not instantaneous', async () => {
      const channel = new EventEmitter();
      createChannelStub.onCall(0).resolves(channel);
      createChannelStub.rejects(new Error('amqplib.createChannel() called more than once.'));

      // Get three connections in parallel
      const [ conn1, conn2, conn3 ] = await Promise.all([ rabbit._getChannel(), rabbit._getChannel(), rabbit._getChannel() ]);

      expect(conn1).to.equal(channel);
      expect(conn2).to.equal(channel);
      expect(conn3).to.equal(channel);

      expect(createChannelStub.calledOnce).to.equal(true);

      // Try again with three more connections, expect the caching to have already happened
      const [ conn4, conn5, conn6 ] = await Promise.all([ rabbit._getChannel(), rabbit._getChannel(), rabbit._getChannel() ]);

      expect(conn4).to.equal(channel);
      expect(conn5).to.equal(channel);
      expect(conn6).to.equal(channel);

      expect(createChannelStub.calledOnce).to.equal(true);

      expect(_onChannelOpenedStub.calledOnce).to.equal(true);
      expect(_onChannelOpenedStub.firstCall.args).to.deep.equal([ channel ]);
    });

    it('should create a channel, and drop it if an error occurs', async () => {
      const channel = new EventEmitter();

      createChannelStub.resolves(channel);

      // First call, should call amqplib.createChannel()
      expect(await rabbit._getChannel()).to.equal(channel);
      expect(createChannelStub.calledOnce).to.equal(true);

      // Emit an error, expecting the connection cache to be flushed
      channel.emit('error', new Error('Test channel error'));
      // Wait a bit to ensure that the event is caught
      await new Promise(resolve => setImmediate(resolve));

      // Second call, should call amqplib.createChannel() again
      expect(await rabbit._getChannel()).to.equal(channel);
      expect(createChannelStub.calledTwice).to.equal(true);

      // _onChannelOpened() should be called twice, one for each channel opened
      expect(_onChannelOpenedStub.calledTwice).to.equal(true);
      expect(_onChannelOpenedStub.firstCall.args).to.deep.equal([ channel ]);
      expect(_onChannelOpenedStub.secondCall.args).to.deep.equal([ channel ]);
    });

    it('should create a channel, and then delete cached channel and call _onChannelClosed() if channel emits \'close\' event', async () => {
      const channel = new EventEmitter();

      createChannelStub.resolves(channel);

      expect(await rabbit._getChannel()).to.equal(channel);

      // Emit a close event, expecting the connection cache to be flushed
      channel.emit('close');
      // Wait a bit to ensure that the event is caught
      await new Promise(resolve => setImmediate(resolve));

      expect(_onChannelClosedStub.calledOnce).to.equal(true);

      // Cached channel should be gone
      expect(rabbit._channel).to.equal(undefined);
    });

  });

});