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
var fs = require('fs');
var EventProxy = require('eventproxy');
var should = require('should');
var utils = require('./support/utils');
var Client = require('../lib/client');
var HConstants = require('../lib/hconstants');
var Get = require('../lib/get');
var Put = require('../lib/put');
var config = require('./config');
var interceptor = require('interceptor');
var HRegionInfo = require('../lib/hregion_info');
var Scan = require('../lib/scan');
var DataInputBuffer = require('../lib/data_input_buffer');
var Bytes = require('../lib/util/bytes');
var HRegionLocation = require('../lib/hregion_location');
var Result = require('../lib/result');
var Delete = require('../lib/delete');
var filters = require('../').filters;

// we need to block it on localhost.. otherwise it's too quick
var blockMe = function (ms) {
  var d = new Date();
  while(new Date() - d < ms) {
    continue;
  }
};

describe('test/client.test.js', function () {
  this.timeout(30000);

  config.clusters.forEach(function (zkConfig) {
  describe('cluster ' + zkConfig.zookeeperRoot, function () {
    zkConfig.logger = config.logger;
    zkConfig.rpcTimeout = config.rpcTimeout;

    var client = null;
    before(function () {
      client = Client.create(zkConfig);
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
          // regionLocation.hostname.should.include(config.hostnamePart);
          regionLocation.port.should.match(/^\d+$/);
          // regionLocation.port.should.equal(36020);
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
          // regionLocation.hostname.should.include(config.hostnamePart);
          regionLocation.port.should.match(/^\d+$/);
          regionLocation.should.have.property('regionInfo');
          // console.log(regionLocation);
          done();
        });
      });

      it('should locate a region with table and row', function (done) {
        // tcif_acookie_actions, f390MDAwMDAwMDAwMDAwMDAxOQ==
        var table = new Buffer(config.tableUser);
        var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
        client.locateRegion(table, row, function (err, regionLocation) {
          should.not.exists(err);
          // {"hostname":"dw48.kgb.sqa.cm4","port":36020,"startcode":1366598005029}
          // regionLocation.hostname.should.include(config.hostnamePart);
          regionLocation.port.should.match(/^\d+$/);
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
        var table = new Buffer(config.tableUser);
        var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
        var count = 0;
        var mockGetClosestRowBefore = function (regions, r, info, callback) {
          var self = this;
          process.nextTick(function () {
            if (r.toString().indexOf(config.tableUser) >= 0 && count === 0) {
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
          // regionLocation.hostname.should.include(config.hostnamePart);
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
              // regionLocation3.hostname.should.include(config.hostnamePart);
              regionLocation3.should.have.property('regionInfo');
              done();
            });
          });

        });

      });

      it('should return null when offline error happen more than retries', function (done) {
        var table = new Buffer(config.tableUser);
        var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
        var count = 0;
        var mockGetClosestRowBefore = function (regions, r, info, callback) {
          var self = this;
          process.nextTick(function () {
            if (r.toString().indexOf(config.tableUser) >= 0 && count === 0) {
              var err = new Error('RegionOfflineException haha');
              err.name = 'RegionOfflineException';
              return callback(err);
            }
            self.____getClosestRowBefore(regions, r, info, callback);
          });
        };

        client.locateRegion(table, row, true, function (err, regionLocation) {
          should.not.exists(err);
          // regionLocation.hostname.should.include(config.hostnamePart);
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
        client.getRow(config.tableUser, 'f390MDAwMDAwMDAwMDAwMDAxOQ==', ['cf1:history', 'cf1:qualifier2'],
        function (err, r) {
          should.not.exists(err);
          //should.exist(r);

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
            client.getRow(config.tableUser, 'f390MDAwMDAwMDAwMDAwMDAxOQ==', ['cf1:history', 'cf1:qualifier2'],
            function (err, r) {
              should.not.exists(err);
              // should load remove regions again
              // for (var i = 0; i < closeRS.length; i++) {
              //   client.cachedServers[closeRS[i]].should.equal(true);
              // }
              done();
            });
          }, 1000);
        });
      });
    });

    describe('getRegionConnection()', function () {

      var region;

      before(function (done) {
        var table = new Buffer(config.tableUser);
        var row = new Buffer('f390MDAwMDAwMDAwMDAwMDAxOQ==');
        client.locateRegion(table, row, function (err, regionLocation) {
          should.not.exists(err);
          // regionLocation.hostname.should.include(config.hostnamePart);
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
        blockMe(10);
      });
    });

    describe('getRow(table, row, columns)', function () {
      var table = config.tableUser;
      var rows = [
        'e0abMDAwMDAwMDAwMDAwMDAxNQ==',
        '4edaMDAwMDAwMDAwMDAwMDAxNg==',
        '7c32MDAwMDAwMDAwMDAwMDAxNw==',
        '0ed7MDAwMDAwMDAwMDAwMDAxOA==',
        'f390MDAwMDAwMDAwMDAwMDAxOQ==',
      ];

      before(function (done) {
        done = pedding(rows.length, done);
        rows.forEach(function (r) {
          client.putRow(table, r, {'cf1:history': r + ' cf1:history', 'cf1:qualifier2': r + ' cf1:qualifier2'}, done);
        });
      });

      it('should get a row with columns', function (done) {
        done = pedding(rows.length, done);
        rows.forEach(function (row) {
          client.getRow(table, row, ['cf1:history', 'cf1:qualifier2'], function (err, r) {
            should.not.exist(err);
            should.exist(r);
            r.should.have.keys('cf1:history', 'cf1:qualifier2');
            for (var k in r) {
              r[k].toString().should.equal(row + ' ' + k);
            }
            done();
          });
        });
      });

      it('should get a row with all columns (select *)', function (done) {
        done = pedding(rows.length * 4, done);

        rows.forEach(function (row) {
          client.getRow(table, row, null, function (err, r) {
            should.not.exists(err);
            should.exist(r);
            r.should.have.keys('cf1:history', 'cf1:qualifier2');
            for (var k in r) {
              r[k].toString().should.equal(row + ' ' + k);
            }
            done();
          });
          client.getRow(table, row, '*', function (err, r) {
            should.not.exists(err);
            should.exist(r);
            r.should.have.keys('cf1:history', 'cf1:qualifier2');
            for (var k in r) {
              r[k].toString().should.equal(row + ' ' + k);
            }
            done();
          });
          client.getRow(table, row, [], function (err, r) {
            should.not.exists(err);
            should.exist(r);
            r.should.have.keys('cf1:history', 'cf1:qualifier2');
            for (var k in r) {
              r[k].toString().should.equal(row + ' ' + k);
            }
            done();
          });
          client.getRow(table, row, function (err, r) {
            should.not.exists(err);
            should.exist(r);
            r.should.have.keys('cf1:history', 'cf1:qualifier2');
            for (var k in r) {
              r[k].toString().should.equal(row + ' ' + k);
            }
            done();
          });
        });
      });

      it('should get empty when row not exists', function (done) {
        var rs = [
          '1234e0abMDAwMDAwMDAwMDAwMDAxNQ==not',
          '45674edaMDAwMDAwMDAwMDAwMDAxNg==not',
          '67897c32MDAwMDAwMDAwMDAwMDAxNw==not',
          '92340ed7MDAwMDAwMDAwMDAwMDAxOA==not',
          '2543f390MDAwMDAwMDAwMDAwMDAxOQ==not',
        ];
        done = pedding(rs.length, done);

        rs.forEach(function (row) {
          client.getRow(table, row, ['cf1:history', 'cf1:qualifier2'], function (err, r) {
            should.not.exists(err);
            should.not.exists(r);
            done();
          });
        });
      });

      it('should get NoSuchColumnFamilyException when Column family not exists', function (done) {
        client.getRow(table, '123123', ['foo:notexists'], function (err, r) {
          should.exists(err);
          err.name.should.equal('org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException');
          err.message.should.include('org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException: Column family foo does not exist in region ' + config.tableUser);
          should.not.exists(r);
          done();
        });
      });

      it('should getRow and convert buffer to json', function (done) {
        client.putRow(table, 'json-data', {'cf1:name': 'foo name', 'cf1:age': '18'}, function (err) {
          should.not.exist(err);
          client.getRow(table, 'json-data', function (err, row) {
            should.not.exist(err);
            should.exist(row);
            row['cf1:name'].toString().should.equal('foo name');
            row['cf1:age'].toString().should.equal('18');
            done();
          });
        });
      });
    });

    describe('get(table, get)', function () {
      var table = config.tableUser;
      var rows = [
        'get-e0abMDAwMDAwMDAwMDAwMDAxNQ==',
        'get-4edaMDAwMDAwMDAwMDAwMDAxNg==',
        'get-7c32MDAwMDAwMDAwMDAwMDAxNw==',
        'get-0ed7MDAwMDAwMDAwMDAwMDAxOA==',
        'get-f390MDAwMDAwMDAwMDAwMDAxOQ==',
      ];

      before(function (done) {
        done = pedding(rows.length, done);
        rows.forEach(function (r) {
          client.putRow(table, r, {'cf1:history': r + ' cf1:history', 'cf1:qualifier2': r + ' cf1:qualifier2'}, done);
        });
      });

      it('should get a row with cf1: from a table', function (done) {
        done = pedding(rows.length * 2, done);

        rows.forEach(function (row) {
          var get = new Get(row);
          get.addColumn('cf1', 'history');
          get.addColumn('cf1', 'qualifier2');
          // get.maxVersions = 1;
          client.get(table, get, function (err, result) {
            should.not.exists(err);
            var kvs = result.raw();
            kvs.length.should.above(0);
            for (var i = 0; i < kvs.length; i++) {
              var kv = kvs[i];
              kv.getRow().toString().should.equal(row);
              kv.getValue().toString().should.include(row);
            }
            done();
          });
        });

        rows.forEach(function (row) {
          client.getRow(table, row, ['cf1:history', 'cf1:qualifier2'], function (err, data) {
            should.not.exists(err);
            data.should.have.keys('cf1:history', 'cf1:qualifier2');
            data['cf1:history'].toString().should.include(row);
            data['cf1:qualifier2'].toString().should.include(row);
            done();
          });
        });
      });

      it('should get empty when row not exists', function (done) {
        var rs = [
          'get-1234e0abMDAwMDAwMDAwMDAwMDAxNQ==not',
          'get-45674edaMDAwMDAwMDAwMDAwMDAxNg==not',
          'get-67897c32MDAwMDAwMDAwMDAwMDAxNw==not',
          'get-92340ed7MDAwMDAwMDAwMDAwMDAxOA==not',
          'get-2543f390MDAwMDAwMDAwMDAwMDAxOQ==not',
        ];
        done = pedding(rs.length, done);

        rs.forEach(function (row) {
          var get = new Get(row);
          get.addColumn('cf1', 'history');
          get.addColumn('cf1', 'qualifier2');
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

      describe.skip('mock org.apache.hadoop.hbase.NotServingRegionException', function () {

        beforeEach(function (done) {
          var table = config.tableActions;
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
          var table = config.tableActions;
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
                mm(server, 'in', utils.createNotServingRegionExceptionBuffer(counter));
              }
              mm(client, 'maxActionRetries', 0);

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
      before(function (done) {
        var table = config.tableUser;
        var rows = [
          'scanner-row0',
          'scanner-row1',
          'scanner-row2',
          'scanner-row3',
          'scanner-row4',
          'scanner-row5',
        ];
        done = pedding(rows.length, done);
        rows.forEach(function (r) {
          client.putRow(table, r, {'cf1:history': r + ' cf1:history', 'cf1:qualifier2': r + ' cf1:qualifier2'}, done);
        });
      });

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
        var tableName = Bytes.toBytes(config.tableUser);
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
                // maybe fails when region spliting
                // OFFLINE => true, SPLIT => true
                // console.log(loc.toString());
                if (!location.offLine) {
                  loc.toString().should.equal(location.toString());
                  loc.hostname.should.equal(location.hostname);
                  loc.port.should.equal(location.port);
                  loc.regionInfo.startKey.should.eql(location.regionInfo.startKey);
                  loc.regionInfo.endKey.should.eql(location.regionInfo.endKey);
                }

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
        var tableName = Bytes.toBytes(config.tableUser);
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

      it('should scan rows one by one with filter: only return row key, no values',
      function (done) {
        var tableName = Bytes.toBytes(config.tableUser);
        var scan = new Scan('scanner-row0', 'scanner-row5');
        var filterList = new filters.FilterList(filters.FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new filters.FirstKeyOnlyFilter());
        filterList.addFilter(new filters.KeyOnlyFilter());
        scan.setFilter(filterList);

        client.getScanner(config.tableUser, scan, function (err, scanner) {
          should.not.exists(err);
          should.exists(scanner);
          scanner.should.have.property('id').with.be.instanceof(Long);
          scanner.should.have.property('server');

          var index = 0;

          var next = function (numberOfRows) {
            scanner.next(numberOfRows, function (err, rows) {
              // console.log(rows)
              should.not.exists(err);
              if (rows.length === 0) {
                index.should.equal(5);
                return scanner.close(done);
              }

              rows.should.length(1);

              var closed = false;
              rows.forEach(function (row) {
                var kvs = row.raw();
                var r = {};
                for (var i = 0; i < kvs.length; i++) {
                  var kv = kvs[i];
                  kv.getRow().toString().should.equal('scanner-row' + index++);
                  kv.toString().should.include('/vlen=0/');
                  // console.log(kv.getRow().toString(), kv.toString());
                }
              });


              if (closed) {
                return scanner.close(done);
              }

              next(numberOfRows);
            });
          };
          next(1);
        });
      });

      it('should scan rows with filter: only return row key, value is length',
      function (done) {
        var tableName = Bytes.toBytes(config.tableUser);
        var scan = new Scan('scanner-row0', 'scanner-row5');
        var filterList = new filters.FilterList(filters.FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new filters.FirstKeyOnlyFilter());
        filterList.addFilter(new filters.KeyOnlyFilter(true));
        scan.setFilter(filterList);

        client.getScanner(config.tableUser, scan, function (err, scanner) {
          should.not.exists(err);
          should.exists(scanner);
          scanner.should.have.property('id').with.be.instanceof(Long);
          scanner.should.have.property('server');

          var index = 0;

          var next = function (numberOfRows) {
            scanner.next(numberOfRows, function (err, rows) {
              // console.log(rows)
              should.not.exists(err);
              if (rows.length === 0) {
                index.should.equal(5);
                return scanner.close(done);
              }

              rows.should.length(1);

              var closed = false;
              rows.forEach(function (row) {
                var kvs = row.raw();
                var r = {};
                for (var i = 0; i < kvs.length; i++) {
                  var kv = kvs[i];
                  kv.getRow().toString().should.equal('scanner-row' + index++);
                  var len = kv.getValue().readUInt32BE(0);
                  // console.log('%j, %j, %j, %d',
                  //   kv.getRow().toString(), kv.toString(), kv.getValue().toString(), len);
                  kv.toString().should.include('/vlen=4/');
                  len.should.equal(24);
                }
              });


              if (closed) {
                return scanner.close(done);
              }

              next(numberOfRows);
            });
          };
          next(1);
        });
      });

      it('should scan rows with filter: single column value filter',
      function (done) {
        var tableName = Bytes.toBytes(config.tableUser);
        var family = 'cf1';
        var qualifier = 'qualifier2';
        var scan = new Scan('scanner-row0', 'scanner-row5');
        var filterList = new filters.FilterList(filters.FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'LESS_OR_EQUAL', 'scanner-row0 cf1:qualifier2'));
        filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'GREATER_OR_EQUAL', new filters.BinaryPrefixComparator('scanner-')));
        filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'NOT_EQUAL', new filters.BitComparator('0', filters.BitComparator.BitwiseOp.XOR)));
        filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'NOT_EQUAL', new filters.NullComparator()));
        filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'EQUAL', new filters.RegexStringComparator('scanner-*')));
        filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'EQUAL', new filters.SubstringComparator('cf1:qualifier2')));
        scan.setFilter(filterList);

        client.getScanner(config.tableUser, scan, function (err, scanner) {
          should.not.exists(err);
          should.exists(scanner);
          scanner.should.have.property('id').with.be.instanceof(Long);
          scanner.should.have.property('server');

          var index = 0;

          var next = function (numberOfRows) {
            scanner.next(numberOfRows, function (err, rows) {
              should.not.exists(err);
              if (rows.length === 0) {
                index.should.equal(1);
                return scanner.close(done);
              }

              rows.should.length(1);

              var closed = false;
              rows.forEach(function (row) {
                var kvs = row.raw();
                var r = {};
                var isMatched = false;
                for (var i = 0; i < kvs.length; i++) {
                  var kv = kvs[i];
                  kv.getRow().toString().should.equal('scanner-row0');
                  var value = kv.getValue().toString();
                  if (value.indexOf('scanner-') == 0 && value.indexOf('cf1:qualifier2') != -1) {
                    isMatched = true
                  }
                  var len = kv.getValue().readUInt32BE(0);
                  // console.log('%j, %j, %j, %d',
                  //   kv.getRow().toString(), kv.toString(), kv.getValue().toString(), len);
                }
                isMatched.should.equal(true);
              });

              if (closed) {
                return scanner.close(done);
              }

              index++;
              next(numberOfRows);
            });
          };
          next(1);
        });
      });

    });

    describe('put(table, put)', function () {

      it('should put a row with cf1: to a table', function (done) {
        var table = config.tableUser;
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
          put.add('cf1', 'history', 'history: put test 测试数据 ' + row);
          put.add('cf1', 'qualifier2', 'qualifier2: put test 数据2 ' + row);
          client.put(table, put, function (err, result) {
            // console.log(arguments)
            should.not.exists(err);
            should.not.exists(result);

            var get = new Get(row);
            get.addColumn('cf1', 'history');
            get.addColumn('cf1', 'qualifier2');
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
            'cf1:history': 'history: put test 测试数据 ' + row,
            'cf1:qualifier2': 'qualifier2: put test 数据2 ' + row,
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
      var table = config.tableUser;
      afterEach(function () {
        client.deleteRow(table, rowkey, function (err, result) {
          should.not.exists(err);
        }); // delete
      });

      it('simple delete by row', function (done) {
        var data = {'cf1:name-t': 't-test01', 'cf1:value-t': 't-test02'};
        client.putRow(table, rowkey, data, function (err) {
          should.not.exists(err);
          client.getRow(table, rowkey, ['cf1:name-t', 'cf1:value-t'], function (err, result) {
            should.not.exists(err);
            should.exists(result);
            result.should.have.keys('cf1:name-t', 'cf1:value-t');
            client.deleteRow(table, rowkey, function (err, result) {
              should.not.exists(err);
              client.getRow(table, rowkey, ['cf1:name-t', 'cf1:value-t'], function (err, result) {
                should.not.exists(err);
                should.not.exists(result);
                done();
              }); // get
            }); // delete
          }); // get
        }); // put
      }); // it

      describe('delete columns', function () {
        var data = {'cf1:name-t': 't-test01', 'cf1:value-t': 't-test02'};
        var columns = ['cf1:name-t', 'cf1:value-t'];
        var rowkey = '58c8MDAwMDAwMDAwMDAwMDAwMQ==1';
        before(function (done) {
          client.putRow(table, rowkey, data, function (err) {
            should.not.exists(err);
            setTimeout(done, 100);
          });
        });
        after(function () {
          client.deleteRow(table, rowkey, function (err, result) {
            should.not.exists(err);
          }); // delete
        });

        it('should work', function (done) {
          client.getRow(table, rowkey, columns, function (err, result) {
            should.not.exists(err);
            should.exists(result);
            result.should.have.keys('cf1:name-t', 'cf1:value-t');
            var del = new Delete(rowkey);
            del.deleteColumns('cf1', 'name-t');
            client.delete(table, del, function (err, result) {
              should.not.exists(err);
              client.getRow(table, rowkey, columns, function (err, result) {
                should.not.exists(err);
                should.exists(result);
                result.should.have.keys('cf1:value-t');
                done();
              }); // get
            }); // delete
          }); // get
        }); // it
      });

      it('delete column latest version', function (done) {
        var data = {'cf1:name-t': 't-test01', 'cf1:value-t': 't-test02'};
        var columns = ['cf1:name-t', 'cf1:value-t'];
        client.putRow(table, rowkey, data, function (err, result) {
          should.not.exists(err);
          setTimeout(() => {
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
                var rs = result.getColumn('cf1', 'name-t');
                rs.length.should.eql(2);
                rs.forEach(function (kv) {
                  kv.getValue().toString().should.eql('t-test01');
                });
                //result.should.have.keys('f:name-t', 'f:value-t');
                var del = new Delete(rowkey);
                del.deleteColumn('cf1', 'name-t');
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
                    var rs = result.getColumn('cf1', 'name-t');
                    rs.length.should.eql(1);
                    rs.forEach(function (kv) {
                      kv.getValue().toString().should.eql('t-test01');
                    });
                    done();
                  }); // get
                }); // delete
              }); // get
            }); // put version2
          }, 10);
        }); // put version1
      });

    });

    describe('mget', function () {
      var tableName = config.tableUser;
      var columns = ['cf1:history'];
      it('should get 1 row from table', function (done) {
        var rows = ['a98eMDAwMDAwMDAwMDAwMDAwMg==single'];
        client.putRow(tableName, rows[0], {'cf1:history': '123'}, function (err, result) {
          should.not.exists(err);
          client.mget(tableName, rows, columns, function (err, result) {
            should.not.exists(err);
            should.exists(result);
            result.length.should.eql(1);
            result[0].should.have.keys('cf1:history');
            result[0]['cf1:history'].toString().should.eql('123');
            done();
          });
        });
      });

      it('should get rows and big content from table', function (done) {
        var rows = [
          '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
          '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
          '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
          'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
          '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
          '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
          '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
          'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
          '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
          '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
          '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
          'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
          '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
          '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
          '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
          'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
          '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
          '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
          '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
          'a98eMDAwMDAwMDAwMDAwMDAwMg==d'
        ];
        var ep = new EventProxy();
        ep.after('put', 4, function () {
          var count = 10;
          done = pedding(count, done);
          var mget = function () {
            client.mget(tableName, rows, ['cf1:history', 'cf1:bigcontent'], function (err, result) {
              should.not.exists(err);
              should.exists(result);
              result.should.length(rows.length);
              // console.log(result);
              result.forEach(function (obj) {
                should.exists(obj);
                obj.should.have.keys('cf1:history', 'cf1:bigcontent');
              });
              done();
            });
          };

          for (var i = 0; i < count; i++) {
            mget();
          }
        });
        var content = fs.readFileSync(__filename);
        rows.slice(0, 4).forEach(function (row, i) {
          client.putRow(tableName, row, {
            'cf1:history': '123' + i,
            'cf1:bigcontent': content
          }, ep.done('put'));
        });
      });
    });

    describe('mput', function () {
      var tableName = config.tableUser;
      var columns = ['cf1:history'];
      it('should put 1 row into table', function (done) {
        client.mput(
          tableName,
          [
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp','cf1:history': 'mput-single'}
          ],
        function (err, result) {
          should.not.exists(err);
          result.length.should.eql(1);
          result[0].constructor.name.should.eql('Result');
          client.mget(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==mp'], ['cf1:history'], function (err, result) {
            should.not.exists(err);
            should.exists(result);
            result.length.should.eql(1);
            result[0].should.have.property('cf1:history');
            result[0]['cf1:history'].toString().should.eql('mput-single');
            done();
          });
        });
      });

      it('should put 2 rows into table', function (done) {
        client.mput(
          tableName,
          [
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp1','cf1:history': 'mput-single1'},
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp2','cf1:history': 'mput-single2'}
          ],
        function (err, result) {
          should.not.exists(err);
          result.length.should.eql(2);
          result[0].constructor.name.should.eql('Result');
          result[1].constructor.name.should.eql('Result');
          client.mget(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==mp1', 'a98eMDAwMDAwMDAwMDAwMDAwMg==mp2'], ['cf1:history'],
          function (err, result) {
            should.not.exists(err);
            should.exists(result);
            result.length.should.eql(2);
            result[0].should.have.keys('cf1:history');
            result[1].should.have.keys('cf1:history');
            result[0]['cf1:history'].toString('utf-8').should.eql('mput-single1');
            result[1]['cf1:history'].toString('utf-8').should.eql('mput-single2');
            done();
          });
        });
      });

    });

    describe('mdelete', function () {
      var tableName = config.tableUser;
      var columns = ['cf1:history'];
      it('should delete 1 rows from table', function (done) {
        client.mput(
          tableName,
          [
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md','cf1:history': 'mdel-single'}
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

      it('should delete 2 rows into table', function (done) {
        client.mput(
          tableName,
          [
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md1','cf1:history': 'mdel-single1'},
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md2','cf1:history': 'mdel-single2'}
          ],
        function (err, result) {
          should.not.exists(err);
          client.mdelete(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==md1', 'a98eMDAwMDAwMDAwMDAwMDAwMg==md2'],
          function (err, result) {
            should.not.exists(err);
            should.exists(result);
            result.length.should.eql(2);
            done();
          });
        });
      });

    });

    describe('mupsert', function () {
      var tableName = config.tableUser;
      it('should insert 1 cell and delete 1 cell from table', function (done) {

        client.mupsert(
          tableName,
          [
            {row: 'a98eMDAwMDAwMDAwMDAwMDAwMg==md','cf1:history': 'upsert-single', 'cf1:qualifier2': null}
          ],
        function (err) {
          should.not.exists(err);

          client.mget(tableName, ['a98eMDAwMDAwMDAwMDAwMDAwMg==md'], ['cf1:history', 'cf1:qualifier2'], function (err, result) {
      should.not.exists(err);
            should.exists(result);
            result.length.should.eql(1);

            result[0].should.have.keys('cf1:history');
            result[0]['cf1:history'].toString().should.eql('upsert-single');
            should.not.exists(result[0]['cf1:qualifier2']);
            done();
          });
        });
      });

    });

  });
  }); // clusters end
});
