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
var Connection = require('../lib/connection');
var Result = require('../lib/result');
var HRegionInfo = require('../lib/hregion_info');
var HConstants = require('../lib/hconstants');
var DataInputBuffer = require('../lib/data_input_buffer');
var Bytes = require('../lib/util/bytes');
var config = require('./config');
var interceptor = require('interceptor');
var ZooKeeperWatcher = require('zookeeper-watcher');

var MAGIC = 255;
var MAGIC_SIZE = Bytes.SIZEOF_BYTE;
var ID_LENGTH_OFFSET = MAGIC_SIZE;
var ID_LENGTH_SIZE = Bytes.SIZEOF_INT;
function removeMetaData(data) {
  if (data === null || data.length === 0) {
    return data;
  }
  // check the magic data; to be backward compatible
  var magic = data[0];
  if (magic !== MAGIC) {
    return data;
  }

  var idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
  var dataLength = data.length - MAGIC_SIZE - ID_LENGTH_SIZE - idLength;
  var dataOffset = MAGIC_SIZE + ID_LENGTH_SIZE + idLength;

  return data.slice(dataOffset, dataOffset + dataLength);
}

describe('test/connection.test.js', function () {

  var connection = null;
  before(function (done) {
    var zk = new ZooKeeperWatcher({
      hosts: config.zookeeperHosts,
      root: config.zookeeperRoot,
      logger: config.logger,
    });
    zk.once('connected', function (err) {

      var rootPath = '/root-region-server';
      zk.watch(rootPath, function (err, value, zstat) {
        var items = removeMetaData(value).toString().split(',');
        if (connection) {
          return;
        }

        connection = new Connection({
          host: items[0],
          port: items[1],
          logger: config.logger,
        });

        connection.on('connect', function () {
          done();
        });
      });
    });
  });

  describe('getProtocolVersion()', function () {
    it('should return protocol version', function (done) {
      connection.getProtocolVersion(null, null, function (err, version) {
        should.not.exists(err);
        version.should.be.an.instanceof(Long);
        version.toNumber().should.equal(29);
        done();
      });
    });

    it('should return protocol version on 20 parallel calls', function (done) {
      done = pedding(20, done);
      var call = function () {
        connection.getProtocolVersion(null, null, function (err, version) {
          should.not.exists(err);
          version.should.be.an.instanceof(Long);
          version.toNumber().should.equal(29);
          done();
        });
      };
      for (var i = 0; i < 20; i++) {
        call();
      }
    });

  });

  describe('getClosestRowBefore()', function () {
    it('should return region info from meta region', function (done) {
      // regionName, row, family
      var regionName = HRegionInfo.ROOT_REGIONINFO.regionName;
      var row = new Buffer('.META.,' + config.tableActions
        + ',f390MDAwMDAwMDAwMDAwMDAxOQ==,99999999999999,99999999999999');
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
        hostAndPort.should.match(/^[\w\-\.]+\:\d+$/);

        // Instantiate the location
        var item = hostAndPort.split(':');
        var hostname = item[0];
        var port = parseInt(item[1], 10);
        // console.log(hostAndPort);
        done();
      });
    });
  });

  describe.skip('mock network error', function () {
    var proxy = interceptor.create(config.regionServer, 100);
    var conn = null;
    var port = 36021;
    beforeEach(function (done) {
      proxy.close();
      proxy.listen(port);
      conn = new Connection({
        host: 'localhost',
        port: port++,
        logger: config.logger,
      });
      done = pedding(2, done);

      conn.once('connect', function () {
        done();
      });
      //must wait server side accept, If do not wait, something unexpected would happened
      proxy.once('_connect', function () {
        // proxy.inStream._connections.should.equal(1);
        done();
      });
    });

    it('should return ECONNREFUSED and emit connectError', function (done) {
      done = pedding(2, done);
      var c = new Connection({
        host: config.invalidHost,
        port: config.invalidPort
      });

      c.getProtocolVersion(null, null, function (err, version) {
        err.name.should.equal('ConnectionRefusedException');
        err.message.should.include('connect ECONNREFUSED');
        should.exists(err);
        should.not.exist(version);
        done();
      });

      c.on('connectError', function (err) {
        err.name.should.equal('ConnectionRefusedException');
        err.message.should.include('connect ECONNREFUSED');
        done();
      });
    });

    it('should return protocol version', function (done) {
      conn.getProtocolVersion(null, null, function (err, version) {
        should.not.exists(err);
        version.should.be.an.instanceof(Long);
        version.toNumber().should.equal(29);
        done();
      });
    });

    it('should return RemoteCallTimeoutException 90ms', function (done) {
      conn.getProtocolVersion(null, null, 90, function (err, version) {
        should.exists(err);
        err.message.should.include('90 ms');
        err.name.should.equal('RemoteCallTimeoutException');
        done();
      });
    });

    it('should return RemoteCallTimeoutException when block before send data',
    function (done) {
      proxy.block();
      conn.getProtocolVersion(null, null, 500, function (err, version) {
        should.exists(err);
        err.message.should.include('500 ms');
        err.name.should.equal('RemoteCallTimeoutException');

        proxy.open();
        // after reopen, because server recive wrong data, server will close the socket.
        conn.getProtocolVersion(null, null, 501, function (err, version) {
          should.exists(err);
          err.name.should.match(/(ConnectionClosedException|RemoteCallTimeoutException)/);
          done();
        });
      });
    });

    it('should return RemoteCallTimeoutException when block after send data', function (done) {
      conn.getProtocolVersion(null, null, 502, function (err, version) {
        should.exists(err);
        err.message.should.include('502 ms');
        err.name.should.equal('RemoteCallTimeoutException');

        proxy.open();
        // after reopen, because server recive wrong data, server will close the socket.
        conn.getProtocolVersion(null, null, 503, function (err, version) {
          should.exists(err);
          err.name.should.match(/(ConnectionClosedException|RemoteCallTimeoutException)/);
          done();
        });
      });
      proxy.block();
    });

    it('should return ConnectionClosedException when remote socket close after send data',
    function (done) {
      // send data before close
      conn.getProtocolVersion(null, null, 1001, function (err, version) {
        should.exists(err);
        err.name.should.equal('ConnectionClosedException');
        // newer nodejs version emits error instead of close.. the message doesn't match
        //err.message.should.include('closed with no error.');

        // send data after close
        conn.getProtocolVersion(null, null, 1002, function (err, version) {
          should.exists(err);
          err.name.should.equal('ConnectionClosedException');
          // newer nodejs version emits error instead of close.. the message doesn't match
          //err.message.should.include('closed with no error.');
          done();
        });
      });
      //close will destory all connections which are already connect to the proxy. same as server force down.
      proxy.close();
    });

    it('should return ConnectionClosedException when client socket end() by itself.',
    function (done) {
      // send data before close
      conn.getProtocolVersion(null, null, 1001, function (err, version) {
        should.exists(err);
        err.name.should.equal('ConnectionClosedException');
        err.message.should.include('closed with no error.');

        // send data after close
        conn.getProtocolVersion(null, null, 1002, function (err, version) {
          should.exists(err);
          err.name.should.equal('ConnectionClosedException');
          err.message.should.include('closed with no error.');
          done();
        });
      });
      conn.socket.end();
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
