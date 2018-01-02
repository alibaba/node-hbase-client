/*!
 * node-hbase-client - lib/scanner.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var async = require('async');
var Linklist = require('algorithmjs').ds.Linklist;

var Result = require('./result');
var Scan = require('./scan');

function Scanner(server, id, scan, region, client, tableName) {
  EventEmitter.call(this);
  
  this.server = server;
  this.id = id;
  this.scan = scan;

  this.closed = false;
  this.cache = new Linklist();

  this.region = region;
  this.client = client;
  this.tableName = tableName;
}

util.inherits(Scanner, EventEmitter);

Scanner.prototype._openNextScanner = function(callback) {
  // no more region
  if (!this.region.endKey || !this.region.endKey.length) {
    return callback(null, false);
  }

  // next region is bigger than scan.stopRow
  if (this.scan.stopRow.length && this.region.endKey.compare(this.scan.stopRow) === 1) {
    return callback(null, false);
  }

  var old = this.scan;
  var scan = new Scan(this.region.endKey, old.stopRow);
  scan.maxVersions = old.maxVersions;
  scan.batch = old.batch;
  scan.caching = old.caching;
  scan.maxResultSize = old.maxResultSize;
  scan.cacheBlocks = old.cacheBlocks;
  scan.filter = old.filter;
  scan.tr = old.tr;
  scan.familyMap = old.familyMap;

  var self = this;
  this.client.getScanner(this.tableName, scan, function(err, scanner) {
    if (err) return callback(err);

    self.server = scanner.server;
    self.id = scanner.id;
    self.scan = scan;
    self.region = scanner.region;

    return callback(null, true);
  });
};

Scanner.prototype._next = function(callback) {
  if (!this.cache.length && this.closed) {
    return callback(null, null);
  }

  if (this.cache.length) {
    return callback(null, this.cache.popFront());
  }

  // cache from server
  var self = this;
  var caching = this.scan.caching < 1 ? 1 : this.scan.caching;
  var countdown = caching;
  var nextFinished = false; 
  var values = [];
  async.whilst(function() {
    return countdown > 0 && !nextFinished;
  }, function(callback) {
    self.server.nextResult(self.id, caching, function(err, rows) {
      if (err) {
        return callback(err);
      }

      if (rows.length) {
        countdown -= rows.length;
        values = Array.prototype.concat.apply(values, rows);
      }

      if (rows.length < caching && countdown > 0) {
        return self._openNextScanner(function(err, opened) {
          if (err) return callback(err);
          nextFinished = !opened;
          callback();
        });
      } else {
        return callback();
      }
    });
  }, function(err) {
    if (err) return callback(err);
    if (!values.length) return callback();

    values.forEach(function(v) {
      self.cache.pushBack(v);
    });

    return callback(null, self.cache.popFront());
  }); 
};

Scanner.prototype.next = function (numberOfRows, callback) {
  if (typeof numberOfRows === 'function') {
    callback = numberOfRows;
    numberOfRows = null;
  }

  if (!numberOfRows) {
    return this._next(callback);
  }

  var self = this;
  var noMore = false;
  var ret = [];
  async.whilst(function() {
    return ret.length < numberOfRows && !noMore;
  }, function(callback) {
    self._next(function(err, v) {
      if (err) return callback(err);
      if (!v) {
        noMore = true;
      } else {
        ret.push(v);
      }
      callback();
    });
  }, function(err) {
    return callback(err, ret);
  });
};

Scanner.prototype.close = function (callback) {
  var self = this;
  this.server.closeScanner(this.id, function(err) {
    if (!err) self.closed = true;
    callback.apply(null, arguments);
  });
};

module.exports = Scanner;
