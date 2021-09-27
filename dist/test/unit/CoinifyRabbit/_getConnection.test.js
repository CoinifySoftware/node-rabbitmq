"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const amqplib_1 = __importDefault(require("amqplib"));
const chai_1 = require("chai");
const events_1 = __importDefault(require("events"));
const sinon_1 = __importDefault(require("sinon"));
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_getConnection', () => {
        let _generateConnectionUrlStub, amqplibConnectStub, rabbit;
        const connectionUrl = 'the-connection-url';
        beforeEach(() => {
            rabbit = new CoinifyRabbit_1.default();
            _generateConnectionUrlStub = sinon_1.default.stub(CoinifyRabbit_1.default, '_generateConnectionUrl');
            _generateConnectionUrlStub.returns(connectionUrl);
            amqplibConnectStub = sinon_1.default.stub(amqplib_1.default, 'connect');
        });
        afterEach(() => {
            _generateConnectionUrlStub.restore();
            amqplibConnectStub.restore();
        });
        it('should create and cache a connection', async () => {
            const conn = new events_1.default();
            amqplibConnectStub.withArgs(connectionUrl).onCall(0).resolves(conn);
            amqplibConnectStub.withArgs(connectionUrl).rejects(new Error('amqplib.connect() called more than once.'));
            (0, chai_1.expect)(await rabbit._getConnection()).to.equal(conn);
            (0, chai_1.expect)(amqplibConnectStub.calledOnce).to.equal(true);
            (0, chai_1.expect)(await rabbit._getConnection()).to.equal(conn);
            (0, chai_1.expect)(amqplibConnectStub.calledOnce).to.equal(true);
        });
        it('should create and cache a single connection, even if creating the connection is not instantaneous', async () => {
            const conn = new events_1.default();
            amqplibConnectStub.withArgs(connectionUrl).onCall(0).resolves(conn);
            amqplibConnectStub.withArgs(connectionUrl).rejects(new Error('amqplib.connect() called more than once.'));
            const [conn1, conn2, conn3] = await Promise.all([rabbit._getConnection(), rabbit._getConnection(), rabbit._getConnection()]);
            (0, chai_1.expect)(conn1).to.equal(conn);
            (0, chai_1.expect)(conn2).to.equal(conn);
            (0, chai_1.expect)(conn3).to.equal(conn);
            (0, chai_1.expect)(amqplibConnectStub.calledOnce).to.equal(true);
            const [conn4, conn5, conn6] = await Promise.all([rabbit._getConnection(), rabbit._getConnection(), rabbit._getConnection()]);
            (0, chai_1.expect)(conn4).to.equal(conn);
            (0, chai_1.expect)(conn5).to.equal(conn);
            (0, chai_1.expect)(conn6).to.equal(conn);
            (0, chai_1.expect)(amqplibConnectStub.calledOnce).to.equal(true);
        });
        it('should create a connection, and drop it if an error occurs', async () => {
            const conn = new events_1.default();
            amqplibConnectStub.withArgs(connectionUrl).resolves(conn);
            (0, chai_1.expect)(await rabbit._getConnection()).to.equal(conn);
            (0, chai_1.expect)(amqplibConnectStub.calledOnce).to.equal(true);
            conn.emit('error', new Error('Test connection error'));
            await new Promise(resolve => setImmediate(resolve));
            (0, chai_1.expect)(await rabbit._getConnection()).to.equal(conn);
            (0, chai_1.expect)(amqplibConnectStub.calledTwice).to.equal(true);
        });
    });
});
