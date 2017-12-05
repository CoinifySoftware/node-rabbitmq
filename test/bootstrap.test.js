/*
 * Set up chai
 */
const chai = require('chai');
chai.use(require('chai-as-promised'));
chai.use(require('chai-subset'));

global.expect = chai.expect;
global._ = require('lodash');
