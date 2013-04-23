/*!
 * node-hbase-client - lib/result.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Bytes = require('./util/bytes');
var HbaseObjectWritable = require('./io/hbase_object_writable');
var KeyValue = require('./keyvalue');
var RESULT_VERSION = 1;

function Result() {
  if (!(this instanceof Result)) {
    return new Result();
  }
  this.kvs = null;
  this.familyMap = null;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  this.row = null;
  this.bytes = null;
}

Result.prototype = {
  readFields: function (io) {
    this.familyMap = null;
    this.row = null;
    this.kvs = null;
    var totalBuffer = io.readInt();
    if (totalBuffer === 0) {
      this.bytes = null;
      return;
    }
    var raw = io.read(totalBuffer);
    this.bytes = raw; //new ImmutableBytesWritable(raw, 0, totalBuffer);
  },
  _readFields: function () {
    if (this.bytes === null) {
      this.kvs = [];
      return;
    }
    var buf = this.bytes;
    var offset = 0;
    var finalOffset = buf.length;
    var kvs = [];
    while (offset < finalOffset) {
      var keyLength = Bytes.toInt(buf, offset);
      offset += Bytes.SIZEOF_INT;
      kvs.push(new KeyValue(buf, offset, keyLength));
      offset += keyLength;
    }
    this.kvs = kvs;
  },
  raw: function () {
    if (this.kvs === null) {
      this._readFields();
    }
    return this.kvs;
  },
  size: function () {
    return this.raw().length;
  },
};

Result.prototype.list = Result.prototype.raw;


HbaseObjectWritable.addToClass('Result.class', Result);
module.exports = Result;
