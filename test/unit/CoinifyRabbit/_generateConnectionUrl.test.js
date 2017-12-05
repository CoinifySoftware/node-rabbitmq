'use strict';

const CoinifyRabbit = require('../../../lib/CoinifyRabbit');

describe('CoinifyRabbit', () => {

  describe('#_generateConnectionUrl', () => {

    it('should generate a minimal URL', () => {
      const connectionConfig = {
        protocol: 'amqp',
        host: 'myhost'
      };

      const generatedUrl = CoinifyRabbit._generateConnectionUrl(connectionConfig);

      expect(generatedUrl).to.equal('amqp://myhost');
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

      const generatedUrl = CoinifyRabbit._generateConnectionUrl(connectionConfig);

      expect(generatedUrl).to.equal('amqps://myuser:mypassword@myhost:420/myvhost');
    });

    it('should throw error on invalid protocol', () => {
      const connectionConfig = {
        // Notice the trailing :// in protocol
        protocol: 'amqp://',
        host: 'blond-crocodile.in.cloudamqp.com',
        username: 'jvfzpbxh'
      };

      expect(CoinifyRabbit._generateConnectionUrl.bind(null, connectionConfig)).to.throw('Invalid protocol');
    });


  });

});