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
var Get = require('../').Get;
var Put = require('../').Put;
var config = require('./config');
var interceptor = require('interceptor');

describe('test/client.test.js', function () {

  var client = null;
  before(function () {
    client = Client.create(config);
  });
  
  describe('locateRegion()', function () {
    
    it('should locate root region', function (done) {
      client.locateRegion(HConstants.ROOT_TABLE_NAME, null, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.equal('dw48.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        // console.log(regionLocation);
        done();
      });
    });

    it('should locate meta region', function (done) {
      client.locateRegion(HConstants.META_TABLE_NAME, null, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.equal('dw45.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        // console.log(regionLocation);
        done();
      });
    });

    it('should locate a region with table and row', function (done) {
      // tcif_acookie_actions, f390MDAwMDAwMDAwMDAwMDAxOQ==
      var table = new Buffer('tcif_acookie_actions');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      client.locateRegion(table, row, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.equal('dw48.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        // console.log(regionLocation);
        done();
      });
    });

    it('should locate a region with not exists table', function (done) {
      // tcif_acookie_actions, f390MDAwMDAwMDAwMDAwMDAxOQ==
      var table = new Buffer('not-exists-table');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      client.locateRegion(table, row, true, function (err, regionLocation) {
        should.exists(err);
        err.name.should.equal('TableNotFoundException');
        err.message.should.equal('Table \'not-exists-table\' was not found');
        should.not.exists(regionLocation);
        done();
      });
    });

  });

  describe('getRow(table, row, columns)', function () {
    
    it('should get a row with columns', function (done) {
      var table = 'tcif_acookie_actions';
      var rows = [
        'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
        '4edaMDAwMDAwMDAwMDAwMDAxNg==',
        '7c32MDAwMDAwMDAwMDAwMDAxNw==',
        '0ed7MDAwMDAwMDAwMDAwMDAxOA==',
        'f390MDAwMDAwMDAwMDAwMDAxOQ==',
      ];
      done = pedding(rows.length, done);

      rows.forEach(function (row) {
        client.getRow(table, row, ['f:history', 'f:qualifier2'], function (err, r) {
          should.not.exists(err);
          r.should.have.keys('f:history', 'f:qualifier2');
          for (var k in r) {
            r[k].toString().should.include(row);
          }
          done();
        });
      });
      
    });

    it('should get empty when row not exists', function (done) {
      var table = 'tcif_acookie_actions';
      var rows = [
        '1234e0abMDAwMDAwMDAwMDAwMDAxNQ==not',
        '45674edaMDAwMDAwMDAwMDAwMDAxNg==not',
        '67897c32MDAwMDAwMDAwMDAwMDAxNw==not',
        '92340ed7MDAwMDAwMDAwMDAwMDAxOA==not',
        '2543f390MDAwMDAwMDAwMDAwMDAxOQ==not',
      ];
      done = pedding(rows.length, done);

      rows.forEach(function (row) {
        client.getRow(table, row, ['f:history', 'f:qualifier2'], function (err, r) {
          should.not.exists(err);
          should.not.exists(r);
          done();
        });
      });
      
    });

  });

  describe('get(table, get)', function () {
    
    it('should get a row with f: from a table', function (done) {
      var table = 'tcif_acookie_actions';
      var rows = [
        'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
        '4edaMDAwMDAwMDAwMDAwMDAxNg==',
        '7c32MDAwMDAwMDAwMDAwMDAxNw==',
        '0ed7MDAwMDAwMDAwMDAwMDAxOA==',
        'f390MDAwMDAwMDAwMDAwMDAxOQ==',
      ];
      done = pedding(rows.length, done);

      rows.forEach(function (row) {
        var get = new Get(row);
        get.addColumn('f', 'history');
        get.addColumn('f', 'qualifier2');
        // get.maxVersions = 1;
        client.get(table, get, function (err, result) {
          should.not.exists(err);
          var kvs = result.raw();
          kvs.length.should.above(0);
          for (var i = 0; i < kvs.length; i++) {
            var kv = kvs[i];
            kv.getRow().toString().should.equal(row);
            kv.getValue().toString().should.include(row);
            // console.log(kv.toString(), kv.getValue().toString());
          }
          done();
        });
      });
      
    });

    it('should get empty when row not exists', function (done) {
      var table = 'tcif_acookie_actions';
      var rows = [
        '1234e0abMDAwMDAwMDAwMDAwMDAxNQ==not',
        '45674edaMDAwMDAwMDAwMDAwMDAxNg==not',
        '67897c32MDAwMDAwMDAwMDAwMDAxNw==not',
        '92340ed7MDAwMDAwMDAwMDAwMDAxOA==not',
        '2543f390MDAwMDAwMDAwMDAwMDAxOQ==not',
      ];
      done = pedding(rows.length, done);

      rows.forEach(function (row) {
        var get = new Get(row);
        get.addColumn('f', 'history');
        get.addColumn('f', 'qualifier2');
        // get.maxVersions = 1;
        client.get(table, get, function (err, result) {
          should.not.exists(err);
          var kvs = result.raw();
          kvs.should.length(0);
          should.not.exists(result.bytes);
          // kvs.length.should.above(0);
          // for (var i = 0; i < kvs.length; i++) {
          //   var kv = kvs[i];
          //   kv.getRow().toString().should.equal(row);
          //   kv.getValue().toString().should.include(row);
          //   // console.log(kv.toString(), kv.getValue().toString());
          // }
          done();
        });
      });
      
    });

  });
  
  describe('put(table, put)', function () {
    
    it('should put a row with f: to a table', function (done) {
      var table = 'tcif_acookie_actions';
      var rows = [
        'e0ab1-puttest',
        '4eda2-puttest',
        '7c323-puttest',
        '0ed74-puttest',
        'f3905-puttest',
      ];
      done = pedding(rows.length, done);

      rows.forEach(function (row) {
        var put = new Put(row);
        put.add('f', 'history', 'history: put test 测试数据 ' + row);
        put.add('f', 'qualifier2', 'qualifier2: put test 数据2 ' + row);
        client.put(table, put, function (err, result) {
          // console.log(arguments)
          should.not.exists(err);
          should.not.exists(result);

          var get = new Get(row);
          get.addColumn('f', 'history');
          get.addColumn('f', 'qualifier2');
          client.get(table, get, function (err, result) {
            should.not.exists(err);
            var kvs = result.raw();
            kvs.length.should.above(0);
            for (var i = 0; i < kvs.length; i++) {
              var kv = kvs[i];
              kv.getRow().toString().should.equal(row);
              kv.getValue().toString().should.include(row);
              // console.log(kv.toString(), kv.getValue().toString());
            }
            done();
          });
        });
      });
      
    });

  });


});
