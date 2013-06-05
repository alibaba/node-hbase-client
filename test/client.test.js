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
var Client = require('../lib/client');
var HConstants = require('../lib/hconstants');
var Get = require('../lib/get');
var Put = require('../lib/put');
var config = require('./config_test');
var interceptor = require('interceptor');
var HRegionInfo = require('../lib/hregion_info');
var Scan = require('../lib/scan');
var DataInputBuffer = require('../lib/data_input_buffer');
var Bytes = require('../lib/util/bytes');
var HRegionLocation = require('../lib/hregion_location');
var Result = require('../lib/result');
var EventProxy = require('eventproxy');
var Delete = require('../lib/delete');

describe('test/client.test.js', function () {

  var client = null;
  before(function () {
    client = Client.create(config);
  });

  after(function (done) {
    setTimeout(done, 1000);
  });

  afterEach(mm.restore);

  describe('locateRegion()', function () {
    
    it('should locate root region', function (done) {
      client.locateRegion(HConstants.ROOT_TABLE_NAME, null, true, function (err, regionLocation) {
        should.not.exists(err);
        // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
        // console.log(regionLocation);
        regionLocation.hostname.should.include('.kgb.sqa.cm4');
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
        // console.log(regionLocation);
        regionLocation.hostname.should.include('.kgb.sqa.cm4');
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
            var err = new Error('RegionOfflineException haha');
            err.name = 'RegionOfflineException';
            return callback(err);
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
            var err = new Error('RegionOfflineException haha');
            err.name = 'RegionOfflineException';
            return callback(err);
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
          err.name.should.equal('RegionOfflineException');
          err.message.should.equal('RegionOfflineException haha');
          should.not.exists(regionLocation3);
          done();
        });

      });
      
    });

  });

  describe('mock server close', function () {

    before(function (done) {
      // clean up all servers first
      for (var k in client.servers) {
        var server = client.servers[k];
        server.close();
      }
      setTimeout(done, 500);
    });

    it('should clean all server relation regions cache', function (done) {
      client.getRow('tcif_acookie_user', '1', ['cf1:history', 'cf1:qualifier2'], 
      function (err, r) {
        should.not.exists(err);
        
        var closeRS = [];
        for (var k in client.servers) {
          var server = client.servers[k];
          closeRS.push(server.hostnamePort);
          server.close();
        }

        setTimeout(function () {
          for (var i = 0; i < closeRS.length; i++) {
            should.not.exists(client.cachedServers[closeRS[i]]);
          }
          // console.log(client.cachedServers, closeRS)
          client.getRow('tcif_acookie_user', '1', ['cf1:history', 'cf1:qualifier2'], 
          function (err, r) {
            should.not.exists(err);
            // console.log(client.cachedServers, closeRS)
            // should load remove regions again
            for (var i = 0; i < closeRS.length; i++) {
              client.cachedServers[closeRS[i]].should.equal(true);
            }
            done();
          });
        }, 1000);
      });
    });
  });

  describe('getRegionConnection()', function () {
    
    var region;

    before(function (done) {
      var table = new Buffer('tcif_acookie_user');
      var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
      client.locateRegion(table, row, function (err, regionLocation) {
        should.not.exists(err);
        regionLocation.hostname.should.include('.kgb.sqa.cm4');
        regionLocation.port.should.equal(36020);
        regionLocation.should.have.property('regionInfo');
        region = regionLocation;
        for (var k in client.servers) {
          var server = client.servers[k];
          server.close();
        }
        done();
      });
    });

    it('should connect timeout', function (done) {
      mm(client, 'rpcTimeout', 1);
      client.getRegionConnection(region.hostname, region.port, function (err, server) {
        var rsName = region.hostname + ':' + region.port;
        should.exists(err);
        err.name.should.equal('ConnectionConnectTimeoutException');
        err.message.should.include(rsName + ' connect timeout, 1 ms');
        should.not.exists(server);
        client.serversLength.should.equal(0);
        client.servers.should.not.have.key(rsName);

        mm.restore();

        // connect again will be success
        client.getRegionConnection(region.hostname, region.port, function (err, server) {
          should.not.exists(err);
          should.exists(server);
          client.serversLength.should.equal(1);
          client.servers.should.have.key(rsName);
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

    it('should get a row with all columns (select *)', function (done) {
      var table = 'tcif_acookie_actions';
      var rows = [
        'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
        '4edaMDAwMDAwMDAwMDAwMDAxNg==',
        '7c32MDAwMDAwMDAwMDAwMDAxNw==',
        '0ed7MDAwMDAwMDAwMDAwMDAxOA==',
        'f390MDAwMDAwMDAwMDAwMDAxOQ==',
      ];
      done = pedding(rows.length * 2, done);

      rows.forEach(function (row) {
        client.getRow(table, row, null, function (err, r) {
          should.not.exists(err);
          r.should.have.keys('f:history', 'f:qualifier2');
          for (var k in r) {
            r[k].toString().should.include(row);
          }
          done();
        });
        client.getRow(table, row, '*', function (err, r) {
          should.not.exists(err);
          r.should.have.keys('f:history', 'f:qualifier2');
          for (var k in r) {
            r[k].toString().should.include(row);
          }
          done();
        });
        client.getRow(table, row, [], function (err, r) {
          should.not.exists(err);
          r.should.have.keys('f:history', 'f:qualifier2');
          for (var k in r) {
            r[k].toString().should.include(row);
          }
          done();
        });
        client.getRow(table, row, function (err, r) {
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

    it('should get NoSuchColumnFamilyException when Column family not exists', function (done) {
      client.getRow('tcif_acookie_actions', '123123', ['foo:notexists'], function (err, r) {
        should.exists(err);
        err.name.should.equal('org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException');
        err.message.should.include('org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException: Column family foo does not exist in region tcif_acookie_actions');
        should.not.exists(r);
        done();
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

    describe('mock org.apache.hadoop.hbase.NotServingRegionException', function () {

      beforeEach(function (done) {
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

      afterEach(mm.restore);
      
      it('should return NotServingRegionException', function (doneAll) {
        var table = 'tcif_acookie_actions';
        var rows = [
          'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
          'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
          'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
          'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
          'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
        ];
        var done = pedding(rows.length, function () {
          // get again should be success
          var get = new Get(rows[0]);
          get.addColumn('f', 'history');
          get.addColumn('f', 'qualifier2');
          client.get(table, get, function (err, result) {
            should.not.exists(err);
            var kvs = result.raw();
            kvs.length.should.above(0);
            for (var i = 0; i < kvs.length; i++) {
              var kv = kvs[i];
              kv.getRow().toString().should.equal(rows[0]);
              kv.getValue().toString().should.include(rows[0]);
              // console.log(kv.toString(), kv.getValue().toString());
            }
            doneAll();
          });
        });

        var get = new Get(rows[0]);
        get.addColumn('f', 'history');
        get.addColumn('f', 'qualifier2');
        client.get(table, get, function (err) {
          should.not.exists(err);

          var counter = require('../lib/connection').Call_Counter;
            for (var k in client.servers) {
              var server = client.servers[k];
              // if (k !== 'dw48.kgb.sqa.cm4:36020') {
              //   continue;
              // }
              mm(server, 'in', utils.createNotServingRegionExceptionBuffer(counter));
            }

            rows.forEach(function (row) {
              var get = new Get(row);
              get.addColumn('f', 'history');
              get.addColumn('f', 'qualifier2');
              client.get(table, get, function (err, result) {
                should.exists(err);
                err.name.should.equal('org.apache.hadoop.hbase.NotServingRegionException');
                err.message.should.include('at org.apache.hadoop.hbase.regionserver.HRegionServer.getRegion(HRegionServer.java:3518)');
                should.not.exists(result);
                mm.restore();
                done();                
              });
            });
            
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

  describe('delete(table, delete)', function () {
    var rowkey = '58c8MDAwMDAwMDAwMDAwMDAwMQ==';
    var table = 'tcif_acookie_actions';
    afterEach(function () {
      client.deleteRow(table, rowkey, function (err, result) {
        should.not.exists(err);
      }); // delete
    });

    it('simple delete by row', function (done) {
      var data = {'f:name-t': 't-test01', 'f:value-t': 't-test02'};
      client.putRow(table, rowkey, data, function (err) {
        should.not.exists(err);
        client.getRow(table, rowkey, ['f:name-t', 'f:value-t'], function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.should.have.keys('f:name-t', 'f:value-t');
          client.deleteRow(table, rowkey, function (err, result) {
            should.not.exists(err);
            client.getRow(table, rowkey, ['f:name-t', 'f:value-t'], function (err, result) {
              should.not.exists(err);
              should.not.exists(result);
              done();
            }); // get
          }); // delete
        }); // get
      }); // put
    }); // it

    it('delete columns', function (done) {
      var data = {'f:name-t': 't-test01', 'f:value-t': 't-test02'};
      var columns = ['f:name-t', 'f:value-t'];
      client.putRow(table, rowkey, data, function (err, result) {
        should.not.exists(err);
        client.getRow(table, rowkey, columns,function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.should.have.keys('f:name-t', 'f:value-t');
          var del = new Delete(rowkey);
          del.deleteColumns('f', 'name-t');
          client.delete(table, del, function (err, result) {
            should.not.exists(err);
            client.getRow(table, rowkey, columns, function (err, result) {
              should.not.exists(err);
              should.exists(result);
              result.should.have.keys('f:value-t');
              done();
            }); // get
          }); // delete
        }); // get
      }); // put
    }); // it

    it('delete column latest version', function (done) {
      var data = {'f:name-t': 't-test01', 'f:value-t': 't-test02'};
      var columns = ['f:name-t', 'f:value-t'];
      client.putRow(table, rowkey, data, function (err, result) {
        should.not.exists(err);
        client.putRow(table, rowkey, data, function (err, result) {
          should.not.exists(err);
          var get = new Get(rowkey);
          get.setMaxVersions(2);
          for (var i = 0; i < columns.length; i++) {
            var col = columns[i].split(':');
            get.addColumn(col[0], col[1]);
          }
          client.get(table, get, function (err, result) {
            should.not.exists(err);
            should.exists(result);
            var rs = result.getColumn('f', 'name-t');
            rs.length.should.eql(2);
            rs.forEach(function (kv) {
              kv.getValue().toString().should.eql('t-test01');
            });
            //result.should.have.keys('f:name-t', 'f:value-t');
            var del = new Delete(rowkey);
            del.deleteColumn('f', 'name-t');
            client.delete(table, del, function (err, result) {
              should.not.exists(err);
              var get = new Get(rowkey);
              get.setMaxVersions(2);
              for (var i = 0; i < columns.length; i++) {
                var col = columns[i].split(':');
                get.addColumn(col[0], col[1]);
              }
              client.get(table, get, function (err, result) {
                should.not.exists(err);
                should.exists(result);
                var rs = result.getColumn('f', 'name-t');
                rs.length.should.eql(1);
                rs.forEach(function (kv) {
                  kv.getValue().toString().should.eql('t-test01');
                });
                done();
              }); // get
            }); // delete
          }); // get
        }); // put version2
      }); // put version1
    });

  });

  describe('mget', function () {
    var tableName = 'tcif_acookie_actions';
    var columns = ['f:history'];
    it('get 1 row from table', function (done) {
      var rows = ['a98eMDAwMDAwMDAwMDAwMDAwMg==single'];
      client.putRow(tableName, rows[0], {'f:history': '123'}, function (err, result) {
        should.not.exists(err);
        client.mget(tableName, rows, columns, function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.length.should.eql(1);
          result[0].should.have.keys('f:history');
          result[0]['f:history'].toString('utf-8').should.eql('123');
          done();
        });
      });
    });

    it('get rows from table', function (done) {
      var rows = [
        '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
        '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
        '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
        'a98eMDAwMDAwMDAwMDAwMDAwMg==d'
      ];
      var ep = new EventProxy();
      ep.after('put', 4, function () {
        client.mget(tableName, rows, columns, function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.length.should.eql(4);
          // console.log(result);
          result.forEach(function (obj) {
            should.exists(obj);
            obj.should.have.keys('f:history');
          });
          done();
        });
      });
      rows.forEach(function (row, i) {
        client.putRow(tableName, row, {'f:history': '123' + i}, ep.done('put'));
      });
    });
  });
  
  describe('mput', function () {
    var tableName = 'tcif_acookie_actions';
    var columns = ['f:history'];
    it('put 1 row into table', function (done) {
      client.mput(
        tableName,
        [
          {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp','f:history': 'mput-single'}
        ],
      function (err, result) {
        should.not.exists(err);
        result.length.should.eql(1);
        result[0].constructor.name.should.eql('Result');
        client.mget(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==mp'], ['f:history'], function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.length.should.eql(1);
          result[0].should.have.property('f:history');
          result[0]['f:history'].toString('utf-8').should.eql('mput-single');
          done();
        });
      });
    });

    it('put 2 rows into table', function (done) {
      client.mput(
        tableName,
        [
          {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp1','f:history': 'mput-single1'},
          {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp2','f:history': 'mput-single2'}
        ],
      function (err, result) {
        should.not.exists(err);
        result.length.should.eql(2);
        result[0].constructor.name.should.eql('Result');
        result[1].constructor.name.should.eql('Result');
        client.mget(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==mp1', 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp2'], ['f:history'], function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.length.should.eql(2);
          result[0].should.have.keys('f:history');
          result[1].should.have.keys('f:history');
          result[0]['f:history'].toString('utf-8').should.eql('mput-single1');
          result[1]['f:history'].toString('utf-8').should.eql('mput-single2');
          done();
        });
      });
    });

  });

  describe('mdelete', function () {
    var tableName = 'tcif_acookie_actions';
    var columns = ['f:history'];
    it('delete 1 rows from table', function (done) {
      client.mput(
        tableName,
        [
          {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md','f:history': 'mdel-single'}
        ],
      function (err, result) {
        should.not.exists(err);
        client.mdelete(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==md'], function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.length.should.eql(1);
          done();
        });
      });
    });

    it('delete 2 rows into table', function (done) {
      client.mput(
        tableName,
        [
          {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md1','f:history': 'mdel-single1'},
          {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md2','f:history': 'mdel-single2'}
        ],
      function (err, result) {
        should.not.exists(err);
        client.mdelete(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==md1', 'a98eMDAwMDAwMDAwMDAwMDAwMg==md2'], function (err, result) {
          should.not.exists(err);
          should.exists(result);
          result.length.should.eql(2);
          done();
        });
      });
    });

  });
});
