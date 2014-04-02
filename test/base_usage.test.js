/**!
 * node-hbase-client - test/base_usage.test.js
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
var config = require('./config_test');
var Client = require('../').Client;

describe('base_usage.test.js', function () {
  var client = null;
  before(function () {
    client = Client.create(config);
  });

  after(function (done) {
    setTimeout(done, 1000);
  });

  afterEach(mm.restore);

  describe('mget()', function () {
    var rows = [
      {row: '1', 'cf1:foo': 'bar1'},
      {row: '2', 'cf1:foo': 'bar2'},
      {row: '3', 'cf1:foo': 'bar3'},
      {row: '4', 'cf1:foo': 'bar4'},
      {row: '5', 'cf1:foo': 'bar5'},
      {row: '6', 'cf1:foo': 'bar6'},
      {row: '7', 'cf1:foo': 'bar7'},
      {row: '8', 'cf1:foo': 'bar8'},
      {row: '9', 'cf1:foo': 'bar9'},
      {row: '10', 'cf1:foo': 'bar10'},
      {row: '11', 'cf1:foo': 'bar11'},
      {row: '12', 'cf1:foo': 'bar12'},
      {row: '13', 'cf1:foo': 'bar13'},
      {row: '14', 'cf1:foo': 'bar14'},
      {row: '15', 'cf1:foo': 'bar15'},
      {row: '16', 'cf1:foo': 'bar16'},
    ];

    before(function (done) {
      client.putRow(config.tableUser, rows[0].row, {'cf1:foo': 'test'}, function (err) {
        should.not.exist(err);
        client.mput(config.tableUser, rows, done);
      });
    });

    it('should mget return ordered results', function (done) {
      var keys = rows.map(function (r) {
        return r.row;
      });
      keys.unshift('0');
      keys.push('17');

      client.mget(config.tableUser, keys, ['cf1:foo'], function (err, results) {
        should.not.exist(err);
        results.should.be.an.Array;
        results.should.length(rows.length + 2);
        results.forEach(function (r, i) {
          if (i === 0 || i === 17) {
            should.not.exist(r);
            return;
          }
          r.should.have.keys('cf1:foo');
          r['cf1:foo'].toString().should.equal('bar' + i);
        });
        done();
      });
    });
  });
});
