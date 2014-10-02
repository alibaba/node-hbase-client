/*!
 * node-hbase-client - lib/put.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');
var errors = require('./errors');
var KeyValue = require('./keyvalue');
var Bytes = require('./util/bytes');
var OperationWithAttributes = require('./operation_with_attributes');
var HConstants = require('./hconstants');

/**
 * Used to perform Put operations for a single row.
 * <p>
 * To perform a Put, instantiate a Put object with the row to insert to and
 * for each column to be inserted, execute {@link #add(byte[], byte[], byte[]) add} or
 * {@link #add(byte[], byte[], long, byte[]) add} if setting the timestamp.
 */

var PUT_VERSION = 2;

/**
 * Create a Put operation for the specified row, using a given timestamp, and an existing row lock.
 *
 * @param row row key
 * @param ts timestamp
 * @param rowLock previously acquired row lock, or null
 */
function Put(row, ts, rowLock) {
  OperationWithAttributes.call(this);

  if (row && !Buffer.isBuffer(row)) {
    row = Bytes.toBytes(row);
  }
  if (row === null || row.length > HConstants.MAX_ROW_LENGTH) {
    throw new errors.IllegalArgumentException("Row key is invalid");
  }

  this.row = row;
  this.ts = ts || HConstants.LATEST_TIMESTAMP;
  this.lockId = -1;
  if (rowLock) {
    this.lockId = rowLock.getLockId();
  }
  this.familyMap = {};
  this.writeToWAL = true;
}

util.inherits(Put, OperationWithAttributes);

Put.prototype.getRow = function () {
  return this.row;
};

/**
 * Add the specified column and value, with the specified timestamp as
 * its version to this Put operation.
 *
 * @param family family name
 * @param qualifier column qualifier
 * @param value column value
 * @param [ts] version timestamp
 * @return this
 */
Put.prototype.add = function (family, qualifier, value, ts) {
  var list = this.getKeyValueList(family);
  var kv = this.createPutKeyValue(family, qualifier, ts || this.ts, value);
  list.push(kv);
  return this;
};

/*
 * Create a KeyValue with this objects row key and the Put identifier.
 *
 * @return a KeyValue with this objects row key and the Put identifier.
 */
Put.prototype.createPutKeyValue = function (family, qualifier, ts, value) {
  if (family && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  }
  if (qualifier && !Buffer.isBuffer(qualifier)) {
    qualifier = Bytes.toBytes(qualifier);
  }
  if (value && !Buffer.isBuffer(value)) {
    value = Bytes.toBytes(value);
  }
  return KeyValue.createKeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put, value);
};

/**
 * Creates an empty list if one doesnt exist for the given column family
 * or else it returns the associated list of KeyValue objects.
 *
 * @param family column family
 * @return a list of KeyValue objects, returns an empty list if one doesnt exist.
 */
Put.prototype.getKeyValueList = function (family) {
  var list = this.familyMap[family];
  if (!list) {
    list = this.familyMap[family] = [];
  }
  return list;
};

Put.prototype.write = function (out) {
  out.writeByte(PUT_VERSION);
  Bytes.writeByteArray(out, this.row);
  out.writeLong(this.ts);
  out.writeLong(this.lockId);
  out.writeBoolean(this.writeToWAL);
  out.writeInt(Object.keys(this.familyMap).length);
  for (var family in this.familyMap) {
    Bytes.writeByteArray(out, Bytes.toBytes(family));
    var keys = this.familyMap[family];
    out.writeInt(keys.length);
    var totalLen = 0;
    var j, kv;
    for (j = 0; j < keys.length; j++) {
      kv = keys[j];
      totalLen += kv.getLength();
    }
    out.writeInt(totalLen);
    for (j = 0; j < keys.length; j++) {
      kv = keys[j];
      out.writeInt(kv.getLength());
      out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
    }
  }
  this.writeAttributes(out);
};


module.exports = Put;
