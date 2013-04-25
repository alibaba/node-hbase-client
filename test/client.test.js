/*!
 * node-hbase-client - test/client.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var pedding = require('pedding');
var utils = require('./support/utils');
var should = require('should');
var Client = require('../').Client;
var HConstants = require('../').HConstants;
var config = require('./config');

describe('test/client.test.js', function () {

  var client = null;
  before(function () {
    client = Client.create(config);
  });
  
  describe('locateRegion()', function () {
    
    it.skip('should locate root region', function (done) {
      client.locateRegion(HConstants.ROOT_TABLE_NAME, null, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.equal('dw48.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        console.log(regionLocation);
        done();
      });
    });

    it.skip('should locate a region with table and row', function (done) {
      // tcif_acookie_actions, f390MDAwMDAwMDAwMDAwMDAxOQ==
      var table = new Buffer('tcif_acookie_actions');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      client.locateRegion(table, row, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.equal('dw48.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        console.log(regionLocation);
        done();
      });
    });

  });




});
