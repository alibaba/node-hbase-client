/*!
 * node-hbase-client - test/client.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var mm = require('mm');
var Long = require('long');
var pedding = require('pedding');
var utils = require('./support/utils');
var should = require('should');
var Client = require('../').Client;
var HConstants = require('../').HConstants;
var Get = require('../').Get;
var Put = require('../').Put;
var config = require('./config');
var interceptor = require('interceptor');
var HRegionInfo = require('../').HRegionInfo;
var Scan = require('../').Scan;
var DataInputBuffer = require('../').DataInputBuffer;
var Bytes = require('../').Bytes;
var HRegionLocation = require('../').HRegionLocation;


describe('test/client.test.js', function () {

  var client = null;
  before(function () {
    client = Client.create(config);
  });

  after(function (done) {
    setTimeout(done, 1000);
  });
  
  describe('locateRegion()', function () {
    
    it('should locate root region', function (done) {
      client.locateRegion(HConstants.ROOT_TABLE_NAME, null, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.include('dw48.kgb.sqa.cm4');
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
        regionLocation.hostname.should.include('dw46.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        // console.log(regionLocation);
        done();
      });
    });

    it('should locate a region with table and row', function (done) {
      // tcif_acookie_actions, f390MDAwMDAwMDAwMDAwMDAxOQ==
      var table = new Buffer('tcif_acookie_user');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      client.locateRegion(table, row, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        regionLocation.hostname.should.include('.kgb.sqa.cm4');
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

    it('should relocate table regions when offline error happen', function (done) {
      var table = new Buffer('tcif_acookie_actions');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      var count = 0;
      var mockGetClosestRowBefore = function (regions, r, info, callback) {
        var self = this;
        process.nextTick(function () {
          if (r.toString().indexOf('tcif_acookie_actions') >= 0 && count === 0) {
            count++;
            return callback(new Error('RegionOfflineException'));
          }
          self.____getClosestRowBefore(regions, r, info, callback);
        });
      };
      
      client.locateRegion(table, row, true, function (err, regionLocation) {
        should.not.exists(err);
        regionLocation.hostname.should.include('.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        regionLocation.__test__name = 'regionLocation1';

        client.locateRegion(table, row, true, function (err, regionLocation2) {
          should.not.exists(err);
          should.exists(regionLocation2);
          regionLocation2.should.equal(regionLocation);
          regionLocation2.__test__name.should.equal(regionLocation.__test__name);
          
          // mock server offline
          for (var k in client.servers) {
            var server = client.servers[k];
            server.____getClosestRowBefore = server.getClosestRowBefore;
            mm(server, 'getClosestRowBefore', mockGetClosestRowBefore);
          }

          client.locateRegion(table, row, false, function (err, regionLocation3) {
            mm.restore();
            should.not.exists(err);
            regionLocation3.should.not.have.property('__test__name');
            regionLocation3.hostname.should.include('.kgb.sqa.cm4');
            regionLocation3.port.should.equal(36020);
            regionLocation3.should.have.property('regionInfo');
            done();
          });
        });

      });
      
    });

    it('should return null when offline error happen more than retries', function (done) {
      var table = new Buffer('tcif_acookie_actions');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      var count = 0;
      var mockGetClosestRowBefore = function (regions, r, info, callback) {
        var self = this;
        process.nextTick(function () {
          if (r.toString().indexOf('tcif_acookie_actions') >= 0 && count === 0) {
            return callback(new Error('RegionOfflineException'));
          }
          self.____getClosestRowBefore(regions, r, info, callback);
        });
      };
      
      client.locateRegion(table, row, true, function (err, regionLocation) {
        should.not.exists(err);
        regionLocation.hostname.should.include('.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        regionLocation.__test__name = 'regionLocation1';

        // mock server offline
        for (var k in client.servers) {
          var server = client.servers[k];
          server.____getClosestRowBefore = server.getClosestRowBefore;
          mm(server, 'getClosestRowBefore', mockGetClosestRowBefore);
        }

        client.locateRegion(table, row, false, function (err, regionLocation3) {
          mm.restore();
          should.exists(err);
          err.message.should.equal('RegionOfflineException');
          should.not.exists(regionLocation3);
          done();
        });

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

  describe('getScanner(table, scan)', function () {
    
    var region = function (regionInfoRow) {
      var value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      var io = new DataInputBuffer(value);
      var regionInfo = new HRegionInfo();
      regionInfo.readFields(io);
      value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      var hostAndPort = "";
      if (value !== null) {
        hostAndPort = Bytes.toString(value);
      }
      // Instantiate the location
      var item = hostAndPort.split(':');
      var hostname = item[0];
      var port = parseInt(item[1], 10);
      var location = new HRegionLocation(regionInfo, hostname, port);
      return location;
    };

    it('should scan a table region info in .meta. with next()', function (done) {
      var tableName = Bytes.toBytes('tcif_acookie_user');
      var startRow = HRegionInfo.createRegionName(tableName, 
        HConstants.EMPTY_START_ROW, HConstants.ZEROES, false);
      var scan = new Scan(startRow);
      scan.addFamily(HConstants.CATALOG_FAMILY);
      client.getScanner(HConstants.META_TABLE_NAME, scan, function (err, scanner) {
        should.not.exists(err);
        should.exists(scanner);
        scanner.should.have.property('id').with.be.instanceof(Long);
        scanner.should.have.property('server');

        var count = 0;
        var next = function () {
          scanner.next(function (err, regionInfoRow) {
            should.not.exists(err);
            if (!regionInfoRow) {
              // console.log('total', count);
              return scanner.close(done);
            }
            var location = region(regionInfoRow);
            if (!Bytes.equals(location.regionInfo.tableName, tableName)) {
              console.log('total', count);
              return scanner.close(done);
            }
            count++;
            // console.log(location.toString());
            // console.log(location.regionInfo.startKey.toString(),
            //   location.regionInfo.endKey.toString());
            // should get closet
            client.locateRegion(location.regionInfo.tableName, location.regionInfo.startKey, true, 
            function (err, loc) {
              should.not.exists(err);
              // console.log(loc.toString());
              loc.toString().should.equal(location.toString());
              loc.hostname.should.equal(location.hostname);
              loc.port.should.equal(location.port);
              loc.regionInfo.startKey.should.eql(location.regionInfo.startKey);
              loc.regionInfo.endKey.should.eql(location.regionInfo.endKey);

              if (location.regionInfo.endKey.length === 0) {
                return next();
              }

              // endKey + 1 => next region
              var endKey = new Buffer(location.regionInfo.endKey.toString() + 1);
              // console.log(endKey, endKey.toString())
              client.locateRegion(location.regionInfo.tableName, endKey, true, 
              function (err, loc) {
                should.not.exists(err);
                // console.log(loc.toString());
                // loc.toString().should.equal(location.toString());
                // loc.hostname.should.equal(location.hostname);
                // loc.port.should.equal(location.port);
                loc.regionInfo.startKey.should.eql(location.regionInfo.endKey);
                if (loc.regionInfo.endKey.length > 0) {
                  Bytes.compareTo(loc.regionInfo.endKey, location.regionInfo.endKey).should.above(0);
                }
                next();
              });        
            });            
          });
        };

        next();
        
      });
    });

    it('should scan a table region info in .meta. with next(numberOfRows)', function (done) {
      var tableName = Bytes.toBytes('tcif_acookie_user');
      var startRow = HRegionInfo.createRegionName(tableName, 
        HConstants.EMPTY_START_ROW, HConstants.ZEROES, false);
      var scan = new Scan(startRow);
      scan.addFamily(HConstants.CATALOG_FAMILY);
      client.getScanner(HConstants.META_TABLE_NAME, scan, function (err, scanner) {
        should.not.exists(err);
        should.exists(scanner);
        scanner.should.have.property('id').with.be.instanceof(Long);
        scanner.should.have.property('server');

        var next = function (numberOfRows) {
          scanner.next(numberOfRows, function (err, rows) {
            should.not.exists(err);
            if (rows.length === 0) {
              return scanner.close(done);
            }
            var closed = false;
            rows.forEach(function (regionInfoRow) {
              var location = region(regionInfoRow);
              if (!Bytes.equals(location.regionInfo.tableName, tableName)) {
                closed = true;
                return false;
              }
              // console.log(location.toString())
            });
            
            if (closed) {
              return scanner.close(done);
            }
            
            next(numberOfRows);
          });
        };

        next(10);
        
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
      done = pedding(rows.length * 2, done);

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

        client.putRow(table, row, {
          'f:history': 'history: put test 测试数据 ' + row,
          'f:qualifier2': 'qualifier2: put test 数据2 ' + row,
        }, function (err, result) {
          should.not.exists(err);
          should.not.exists(result);
          done();
        });
      });
      
    });

  });


});
