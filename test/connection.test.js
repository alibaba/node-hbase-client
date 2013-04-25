/*!
 * node-hbase-client - test/connection.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Long = require('long');
var pedding = require('pedding');
var utils = require('./support/utils');
var should = require('should');
var Connection = require('../').Connection;
var ConnectionId = require('../').ConnectionId;
var HRegionInfo = require('../').HRegionInfo;
var HConstants = require('../').HConstants;
var DataInputBuffer = require('../').DataInputBuffer;
var Bytes = require('../').Bytes;
var config = require('./config');

describe('test/connection.test.js', function () {

  var connection = null;
  before(function (done) {
    var remoteId = new ConnectionId({
      host: 'dw48.kgb.sqa.cm4',
      port: '36020',
    }, null, null, 60000);
    connection = new Connection(remoteId);
    connection.setupIOstreams();
    connection.on('connect', function () {
      done();
    });
  });
  
  describe('getProtocolVersion()', function () {
    
    it('should return protocol version', function (done) {
      // var wantGetProtocolVersionSendData = new Buffer([0, 0, 0, 96, 0, 0, 0, 0, 1, 0, 18, 103, 101, 116, 80, 114, 111, 116, 111, 99, 111, 108, 86, 101, 114, 115, 105, 111, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 10, 44, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 104, 97, 100, 111, 111, 112, 46, 104, 98, 97, 115, 101, 46, 105, 112, 99, 46, 72, 82, 101, 103, 105, 111, 110, 73, 110, 116, 101, 114, 102, 97, 99, 101, 6, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);      
      // var wantHeader = new Buffer([44, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 104, 97, 100, 111, 111, 112, 46, 104, 98, 97, 115, 101, 46, 105, 112, 99, 46, 72, 82, 101, 103, 105, 111, 110, 73, 110, 116, 101, 114, 102, 97, 99, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].slice(0, 45));
      connection.getProtocolVersion(null, null, function (err, version) {
        should.not.exists(err);
        version.should.be.an.instanceof(Long);
        version.toNumber().should.equal(29);
        done();
      });
    });

    // it('should locate a region with table and row', function (done) {
    //   // tcif_acookie_actions, f390MDAwMDAwMDAwMDAwMDAxOQ==
    //   var table = new Buffer('tcif_acookie_actions');
    //   var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
    //   client.locateRegion(table, row, true, function (err, regionLocation) {
    //     should.not.exists(err);
    //     // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
    //     regionLocation.hostname.should.equal('dw48.kgb.sqa.cm4');
    //     regionLocation.port.should.equal(36020);
    //     regionLocation.should.have.property('regionInfo');
    //     console.log(regionLocation);
    //     done();
    //   });
    // });

  });

  describe('getClosestRowBefore()', function () {
    it('should return region info from meta region', function (done) {
      // regionName, row, family
      var regionName = HRegionInfo.ROOT_REGIONINFO.regionName;
      var row = new Buffer('.META.,tcif_acookie_actions,f390MDAwMDAwMDAwMDAwMDAxOQ==,99999999999999,99999999999999');
      var family = new Buffer('info');
      connection.getClosestRowBefore(regionName, row, family, function (err, regionInfoRow) {
        should.not.exists(err);
        // var kvs = regionInfoRow.raw();
        // for (var i = 0; i < kvs.length; i++) {
        //   var kv = kvs[i];
        //   console.log(kv.toString());
        // }
        var value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);

        // convert the row result into the HRegionLocation we need!
        var io = new DataInputBuffer(value);
        var regionInfo = new HRegionInfo();
        regionInfo.readFields(io);
        Bytes.equals(regionInfo.getTableName(), HConstants.META_TABLE_NAME).should.equal(true);
        // console.log(regionInfo.getTableName().toString());
        // should.ok(Bytes.equals(regionInfo.getTableName(), tableName));
        regionInfo.isSplit().should.equal(false);
        regionInfo.isOffline().should.equal(false);

        value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        var hostAndPort = "";
        if (value !== null) {
          hostAndPort = Bytes.toString(value);
        }
        hostAndPort.should.match(/^[\w\.]+\:\d+$/);

        // Instantiate the location
        var item = hostAndPort.split(':');
        var hostname = item[0];
        var port = parseInt(item[1], 10);
        // console.log(hostAndPort);
        done();
      });
    });

  });


});


// method: getProtocolVersion
// params: [org.apache.hadoop.hbase.ipc.HRegionInterface, 29]
// clientVersion = 0;
// clientMethodsHash = 0;
// RPC_VERSION = 1
// clazz String.class code: 10
// clazz Long.Type code: 6
// 
// dataLength: 100

// [-34, -83, -66, -17, 0, 0, 0, 0, 1, 0, 18, 103, 101, 116, 80, 114, 111, 116, 111, 99, 111, 108, 86, 101, 114, 115, 105, 111, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 10, 44, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 104, 97, 100, 111, 111, 112, 46, 104, 98, 97, 115, 101, 46, 105, 112, 99, 46, 72, 82, 101, 103, 105, 111, 110, 73, 110, 116, 101, 114, 102, 97, 99, 101, 6, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


// HEADER 45 len
// [44, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 104, 97, 100, 111, 111, 112, 46, 104, 98, 97, 115, 101, 46, 105, 112, 99, 46, 72, 82, 101, 103, 105, 111, 110, 73, 110, 116, 101, 114, 102, 97, 99, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
// 

