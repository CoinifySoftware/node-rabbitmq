'use strict';

const sinon = require('sinon'),
  EventEmitter = require('events');

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

const amqplib = require('amqplib');

describe('CoinifyRabbit', () => {

  describe('#_getConnection', () => {

    let _generateConnectionUrlStub,
      amqplibConnectStub,
      rabbit;

    const connectionUrl = 'the-connection-url';

    beforeEach(() => {
      rabbit = new CoinifyRabbit();

      _generateConnectionUrlStub = sinon.stub(CoinifyRabbit, '_generateConnectionUrl');
      _generateConnectionUrlStub.returns(connectionUrl);

      amqplibConnectStub = sinon.stub(amqplib, 'connect');
    });

    afterEach(() => {
      _generateConnectionUrlStub.restore();
      amqplibConnectStub.restore();
    });

    it('should create and cache a connection', async () => {
      const conn = new EventEmitter();
      amqplibConnectStub.withArgs(connectionUrl).onCall(0).resolves(conn);
      amqplibConnectStub.withArgs(connectionUrl).rejects(new Error('amqplib.connect() called more than once.'));

      // First call, should call amqplib.connect()
      expect(await rabbit._getConnection()).to.equal(conn);
      expect(amqplibConnectStub.calledOnce).to.equal(true);

      // Second call, should use cached connection
      expect(await rabbit._getConnection()).to.equal(conn);
      expect(amqplibConnectStub.calledOnce).to.equal(true);
    });

    it('should create and cache a single connection, even if creating the connection is not instantaneous', async () => {
      const conn = new EventEmitter();
      amqplibConnectStub.withArgs(connectionUrl).onCall(0).resolves(conn);
      amqplibConnectStub.withArgs(connectionUrl).rejects(new Error('amqplib.connect() called more than once.'));

      // Get three connections in parallel
      const [ conn1, conn2, conn3 ] = await Promise.all([ rabbit._getConnection(), rabbit._getConnection(), rabbit._getConnection() ]);

      expect(conn1).to.equal(conn);
      expect(conn2).to.equal(conn);
      expect(conn3).to.equal(conn);

      expect(amqplibConnectStub.calledOnce).to.equal(true);

      // Try again with three more connections, expect the caching to have already happened
      const [ conn4, conn5, conn6 ] = await Promise.all([ rabbit._getConnection(), rabbit._getConnection(), rabbit._getConnection() ]);

      expect(conn4).to.equal(conn);
      expect(conn5).to.equal(conn);
      expect(conn6).to.equal(conn);

      expect(amqplibConnectStub.calledOnce).to.equal(true);
    });

    it('should create a connection, and drop it if an error occurs', async () => {
      const conn = new EventEmitter();

      amqplibConnectStub.withArgs(connectionUrl).resolves(conn);

      // First call, should call amqplib.connect()
      expect(await rabbit._getConnection()).to.equal(conn);
      expect(amqplibConnectStub.calledOnce).to.equal(true);

      // Emit an error, expecting the connection cache to be flushed
      conn.emit('error', new Error('Test connection error'));
      // Wait a bit to ensure that the event is caught
      await new Promise(resolve => setImmediate(resolve));

      // Second call, should call amqplib.connect() again
      expect(await rabbit._getConnection()).to.equal(conn);
      expect(amqplibConnectStub.calledTwice).to.equal(true);
    });

  });

});