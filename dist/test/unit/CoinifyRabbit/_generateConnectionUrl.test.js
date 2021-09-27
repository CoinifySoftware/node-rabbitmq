"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const CoinifyRabbit_1 = __importDefault(require("../../../src/CoinifyRabbit"));
describe('CoinifyRabbit', () => {
    describe('#_generateConnectionUrl', () => {
        it('should generate a minimal URL', () => {
            const connectionConfig = {
                protocol: 'amqp',
                host: 'myhost'
            };
            const generatedUrl = CoinifyRabbit_1.default._generateConnectionUrl(connectionConfig);
            (0, chai_1.expect)(generatedUrl).to.equal('amqp://myhost');
        });
        it('should generate a full-blown URL', () => {
            const connectionConfig = {
                host: 'myhost',
                port: 420,
                vhost: 'myvhost',
                protocol: 'amqps',
                username: 'myuser',
                password: 'mypassword'
            };
            const generatedUrl = CoinifyRabbit_1.default._generateConnectionUrl(connectionConfig);
            (0, chai_1.expect)(generatedUrl).to.equal('amqps://myuser:mypassword@myhost:420/myvhost');
        });
        it('should throw error on invalid protocol', () => {
            const connectionConfig = {
                protocol: 'amqp://',
                host: 'blond-crocodile.in.cloudamqp.com',
                username: 'jvfzpbxh'
            };
            (0, chai_1.expect)(CoinifyRabbit_1.default._generateConnectionUrl.bind(null, connectionConfig)).to.throw('Invalid protocol');
        });
    });
});
