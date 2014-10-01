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
var errors = require('./errors');
var RESULT_VERSION = 1;

function Result(bytes) {
  if (!(this instanceof Result)) {
    return new Result();
  }
  this.kvs = null;
  this.familyMap = null;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  this.row = null;
  this.bytes = bytes || null;
}

Result.prototype.readFields = function (io) {
  this.familyMap = null;
  this.row = null;
  this.kvs = null;
  var totalBuffer = io.readInt();
  if (totalBuffer === 0) {
    this.bytes = null;
    return;
  }
  var raw = io.read(totalBuffer);
  this.bytes = raw;
};

Result.prototype._readFields = function () {
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
};

Result.prototype.raw = function () {
  if (this.kvs === null) {
    this._readFields();
  }
  return this.kvs;
};

Result.prototype.size = function () {
  return this.raw().length;
};

/**
 * Get the latest version of the specified column.
 * @param family family name
 * @param qualifier column qualifier
 * @return value of latest version of column, null if none found
 */
Result.prototype.getValue = function (family, qualifier) {
  var kv = this.getColumnLatest(family, qualifier);
  if (kv === null) {
    return null;
  }
  return kv.getValue();
};

/**
 * Return the KeyValues for the specific column.  The KeyValues are sorted in
 * the {@link KeyValue#COMPARATOR} order.  That implies the first entry in
 * the list is the most recent column.  If the query (Scan or Get) only
 * requested 1 version the list will contain at most 1 entry.  If the column
 * did not exist in the result set (either the column does not exist
 * or the column was not selected in the query) the list will be empty.
 *
 * Also see getColumnLatest which returns just a KeyValue
 *
 * @param {byte[]} family the family
 * @param {byte[]} qualifier
 * @return a list of KeyValues for this column or empty list if the column
 * did not exist in the result set
 */
Result.prototype.getColumn = function (family, qualifier) {
  if (family !== null && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  }
  if (qualifier !== null && !Buffer.isBuffer(qualifier)) {
    qualifier = Bytes.toBytes(qualifier);
  }
  var result = [];
  var kvs = this.raw();
  if (!kvs || kvs.length === 0) {
    return result;
  }
  for (var i = 0; i < kvs.length; i++) {
    var kv = kvs[i];
    if (!Bytes.equals(kv.getFamily(), family)) {
      continue;
    }
    if (!Bytes.equals(kv.getQualifier(), qualifier)) {
      continue;
    }
    result.push(kv);
  }
  return result;
};

/**
 * The KeyValue for the most recent for a given column. If the column does
 * not exist in the result set - if it wasn't selected in the query (Get/Scan)
 * or just does not exist in the row the return value is null.
 *
 * @param family
 * @param qualifier
 * @return KeyValue for the column or null
 */
Result.prototype.getColumnLatest = function (family, qualifier) {
  var kvs = this.raw(); // side effect possibly.
  if (!kvs || kvs.length === 0) {
    return null;
  }

  family = Bytes.toBytes(family);
  qualifier = Bytes.toBytes(qualifier);

  for (var i = 0; i < kvs.length; i++) {
    var kv = kvs[i];
    if (!Bytes.equals(kv.getFamily(), family)) {
      continue;
    }
    if (!Bytes.equals(kv.getQualifier(), qualifier)) {
      continue;
    }
    return kv;
  }
  // var pos = this.binarySearch(kvs, family, qualifier);
  // if (pos === -1) {
  //   return null;
  // }
  // var kv = kvs[pos];
  // if (kv.matchingColumn(family, qualifier)) {
  //   return kv;
  // }
  return null;
};

/**
 * Method for retrieving the row key that corresponds to
 * the row from which this Result was created.
 * @return row
 */
Result.prototype.getRow = function () {
  if (this.row === null) {
    this.raw();
    this.row = this.kvs.length === 0 ? null: this.kvs[0].getRow();
  }
  return this.row;
};

Result.readArray = function (io) {
  // Read version for array form.
  // This assumes that results are sent to the client as Result[], so we
  // have an opportunity to handle version differences without affecting
  // efficiency.
  var version = io.readByte();
  if (version > RESULT_VERSION) {
    throw new errors.IOException("version not supported");
  }
  var numResults = io.readInt();
  if (numResults === 0) {
    return [];
  }
  var results = [];
  var bufSize = io.readInt();
  var buf = new Buffer(bufSize);
  var offset = 0;
  for (var i = 0; i < numResults; i++) {
    var numKeys = io.readInt();
    offset += Bytes.SIZEOF_INT;
    if (numKeys === 0) {
      results[i] = null;
      continue;
    }
    var initialOffset = offset;
    for (var j = 0; j < numKeys; j++) {
      var keyLen = io.readInt();
      Bytes.putInt(buf, offset, keyLen);
      offset += Bytes.SIZEOF_INT;
      var bytes = io.read(keyLen);
      // console.log(keyLen, bytes)
      Bytes.putBytes(buf, offset, bytes);
      // io.readFully(buf, offset, keyLen);
      offset += keyLen;
    }
    var totalLength = offset - initialOffset;
    results[i] = new Result(buf.slice(initialOffset, initialOffset + totalLength));
  }
  return results;
};

Result.prototype.list = Result.prototype.raw;


HbaseObjectWritable.addToClass('Result.class', Result);
module.exports = Result;
