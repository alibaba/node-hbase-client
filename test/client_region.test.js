/**!
 * node-hbase-client - test/client_region.test.js
 *
 * Copyright(c) 2014 Alibaba Group Holding Limited.
 *
 * Authors:
 *   苏千 <suqian.yf@taobao.com> (http://fengmk2.github.com)
 */

"use strict";

/**
 * Module dependencies.
 */

var should = require('should');
var mm = require('mm');
var hbase = require('../');
var config = require('./config');

describe('client_region.test.js', function () {
  var client = null;
  before(function () {
    client = hbase.Client.create(config);
  });

  after(function (done) {
    setTimeout(done, 500);
  });

  afterEach(mm.restore);

  it('should get rowkey from wrong region', function (done) {
    var get = new hbase.Get('0338472dd25d0faeacbef9b957950961');
    get.addColumn('cf', 'w02');
    client.get('bulkwriter_test', get, function (err, result) {
      should.not.exist(err);
      var kvs = result.raw();
      // if (kvs.length > 0) {
      //   for (var i = 0; i < kvs.length; i++) {
      //     var kv = kvs[i];
      //     console.log('kv: %s: %s', kv.getFamily().toString() + ':' + kv.getQualifier().toString(), kv.getValue().toString());
      //   }
      // }
      var location = client.getCachedLocation('bulkwriter_test', get.getRow());
      console.log(location.toString());
      client.getRegionConnection(location.getHostname(), location.getPort(), function (err, server) {
        if (err) {
          return callback(err);
        }

        var get2 = new hbase.Get('fdbf2da2cc85e1c79f953a3d8f482edf');
        get2.addColumn('cf', 'w02');
        server.get(location.getRegionInfo().getRegionName(), get2, function (err, result) {
          should.exist(err);
          err.name.should.equal('org.apache.hadoop.hbase.regionserver.WrongRegionException');
          should.not.exist(result);
          done();
        });
      });
    });
  });

  it('should get wrong region and retry from new region', function (done) {
    var get = new hbase.Get('0338472dd25d0faeacbef9b957950961');
    get.addColumn('cf', 'w02');
    client.get('bulkwriter_test', get, function (err, result) {
      should.not.exist(err);
      var kvs = result.raw();
      // if (kvs.length > 0) {
      //   for (var i = 0; i < kvs.length; i++) {
      //     var kv = kvs[i];
      //     console.log('kv: %s: %s', kv.getFamily().toString() + ':' + kv.getQualifier().toString(), kv.getValue().toString());
      //   }
      // }
      var location = client.getCachedLocation('bulkwriter_test', get.getRow());
      var get2 = new hbase.Get('fdbf2da2cc85e1c79f953a3d8f482edf');
      get2.addColumn('cf', 'w02');
      console.log(location.toString());
      mm(client, 'locateRegion', function (tableName, row, useCache, callback) {
        callback(null, location);
        mm.restore();
      });
      client.get('bulkwriter_test', get2, function (err, result) {
        should.not.exist(err);
        should.exist(result);
        var kvs = result.raw();
        // if (kvs.length > 0) {
        //   for (var i = 0; i < kvs.length; i++) {
        //     var kv = kvs[i];
        //     console.log('kv: %s: %s', kv.getFamily().toString() + ':' + kv.getQualifier().toString(), kv.getValue().toString());
        //   }
        // }
        done();
      });
    });
  });

  it('should error when retry over 3 times', function (done) {
    var get = new hbase.Get('0338472dd25d0faeacbef9b957950961');
    get.addColumn('cf', 'w02');
    client.get('bulkwriter_test', get, function (err, result) {
      should.not.exist(err);
      should.exist(result);
      var location = client.getCachedLocation('bulkwriter_test', get.getRow());
      var get2 = new hbase.Get('fdbf2da2cc85e1c79f953a3d8f482edf');
      get2.addColumn('cf', 'w02');
      mm(client, 'locateRegion', function (tableName, row, useCache, callback) {
        callback(null, location);
      });
      client.get('bulkwriter_test', get2, function (err, result) {
        should.exist(err);
        err.name.should.equal('org.apache.hadoop.hbase.regionserver.WrongRegionException');
        should.not.exist(result);
        done();
      });
    });
  });

  it('should refused error', function (done) {
    var get = new hbase.Get('0338472dd25d0faeacbef9b957950961');
    get.addColumn('cf', 'w02');
    client.get('bulkwriter_test', get, function (err, result) {
      should.not.exist(err);
      should.exist(result);
      var location = client.getCachedLocation('bulkwriter_test', get.getRow());
      mm(location, 'getPort', function () {
        return 64401;
      });

      var get2 = new hbase.Get('fdbf2da2cc85e1c79f953a3d8f482edf');
      get2.addColumn('cf', 'w02');
      mm(client, 'locateRegion', function (tableName, row, useCache, callback) {
        callback(null, location);
      });
      client.get('bulkwriter_test', get2, function (err, result) {
        should.exist(err);
        err.name.should.equal('ConnectionRefusedException');
        should.not.exist(result);
        done();
      });
    });
  });
});
