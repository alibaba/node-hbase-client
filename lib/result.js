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
  if (kvs === null || kvs.length === 0) {
    return null;
  }
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

Result.readArray = function (io) {
  // Read version for array form.
  // This assumes that results are sent to the client as Result[], so we
  // have an opportunity to handle version differences without affecting
  // efficiency.
  var version = io.readByte();
  if (version > RESULT_VERSION) {
    throw new IOException("version not supported");
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

// Result.prototype.binarySearch = function (kvs, family, qualifier) {
//   var searchTerm = KeyValue.createFirstOnRow(kvs[0].getRow(), family, qualifier);

//   // pos === ( -(insertion point) - 1)
//   var pos = Arrays.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
//   // never will exact match
//   if (pos < 0) {
//     pos = (pos + 1) * -1;
//     // pos is now insertion point
//   }
//   if (pos == kvs.length) {
//     return -1; // doesn't exist
//   }
//   return pos;
// };

Result.prototype.list = Result.prototype.raw;


HbaseObjectWritable.addToClass('Result.class', Result);
module.exports = Result;
