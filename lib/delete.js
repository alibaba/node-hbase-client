/*!
 * node-hbase-client - lib/delete.js
 * Copyright(c) 2013 tangyao<tangyao@alibaba-inc.com>
 * MIT Licensed
 */

'use strict';

var util = require('util');
var errors = require('./errors');
var KeyValue = require('./keyvalue');
var Bytes = require('./util/bytes');
var OperationWithAttributes = require('./operation_with_attributes');
var HConstants = require('./hconstants');

var DELETE_VERSION = 3;

/**
 * Create a Delete operation for the specified row and timestamp, using
 * an optional row lock.<p>
 *
 * If no further operations are done, this will delete all columns in all
 * families of the specified row with a timestamp less than or equal to the
 * specified timestamp.<p>
 *
 * This timestamp is ONLY used for a delete row operation.  If specifying
 * families or columns, you must specify each timestamp individually.
 * @param {byte []} row row key
 * @param {long} timestamp maximum version timestamp (only for delete row)
 * @param {RowLock} rowLock previously acquired row lock, or null
 */
function Delete(row, timestamp, rowLock) {
  OperationWithAttributes.call(this);
  if (row && !Buffer.isBuffer(row)) {
    row = Bytes.toBytes(row);
  }
  if (row === null || row.length > HConstants.MAX_ROW_LENGTH) {
    throw new errors.IllegalArgumentException("Row key is invalid");
  }
  this.row = row;
  this.ts = timestamp || HConstants.LATEST_TIMESTAMP;
  this.lockId = -1;
  if (rowLock !== null && typeof rowLock !== 'undefined') {
    this.lockId = rowLock.getLockId();
  }
  this.familyMap = {};
  this.writeToWAL = true;
}

util.inherits(Delete, OperationWithAttributes);

Delete.prototype.getRow = function () {
  return this.row;
};

/**
 * Set the timestamp of the delete.
 *
 * @param {long} timestamp
 */
Delete.prototype.setTimestamp = function (timestamp) {
  this.ts = timestamp;
};

/**
 * Delete all versions of the specified column with a timestamp less than
 * or equal to the specified timestamp.
 * @param {byte []} family family name
 * @param {byte []} qualifier column qualifier
 * @param {long} timestamp maximum version timestamp
 * @return this for invocation chaining
 */
Delete.prototype.deleteColumns = function (family, qualifier, timestamp) {
  if (family !== null && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  }
  if (qualifier !== null && !Buffer.isBuffer(qualifier)) {
    qualifier = Bytes.toBytes(qualifier);
  }
  timestamp = timestamp || HConstants.LATEST_TIMESTAMP;
  var list = this.familyMap[family];
  if (!list) {
    list = this.familyMap[family] = [];
  }
  list.push(KeyValue.createKeyValue(this.row, family, qualifier, timestamp, KeyValue.Type.DeleteColumn));
  return this;
};

/**
 * Delete the latest version of the specified column.
 * This is an expensive call in that on the server-side, it first does a
 * get to find the latest versions timestamp.  Then it adds a delete using
 * the fetched cells timestamp.
 * @param {byte []} family family name
 * @param {byte []} qualifier column qualifier
 * @param {long} timestamp version timestamp
 * @return this for invocation chaining
 */
Delete.prototype.deleteColumn = function (family, qualifier, timestamp) {
  if (family !== null && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  }
  if (qualifier !== null && !Buffer.isBuffer(qualifier)) {
    qualifier = Bytes.toBytes(qualifier);
  }
  timestamp = timestamp || HConstants.LATEST_TIMESTAMP;
  var list = this.familyMap[family];
  if(!list) {
    list = this.familyMap[family] = [];
  }
  list.push(KeyValue.createKeyValue(this.row, family, qualifier, timestamp, KeyValue.Type.Delete));
  return this;
};

/**
 * Delete all columns of the specified family with a timestamp less than
 * or equal to the specified timestamp.
 * <p>
 * Overrides previous calls to deleteColumn and deleteColumns for the
 * specified family.
 * @param {byte []} family family name
 * @param {long} timestamp maximum version timestamp
 * @return this for invocation chaining
 */
Delete.prototype.deleteFamily = function (family, timestamp) {
  if (family !== null && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  }
  timestamp = timestamp || HConstants.LATEST_TIMESTAMP;
  var list = this.familyMap[family] = [];
  list.push(KeyValue.createKeyValue(this.row, family, null, timestamp, KeyValue.Type.DeleteFamily));
  return this;
};

/**
 * Advanced use only.
 * Add an existing delete marker to this Delete object.
 * @param {KeyValue} kv An existing KeyValue of type "delete".
 * @return this for invocation chaining
 * @throws IOException
 */
Delete.prototype.addDeleteMarker = function (kv) {
  // TODO:
};

Delete.prototype.write = function (out) {
  out.writeByte(DELETE_VERSION);
  Bytes.writeByteArray(out, this.row);
  out.writeLong(this.ts);
  out.writeLong(this.lockId);
  out.writeBoolean(this.writeToWAL);
  out.writeInt(Object.keys(this.familyMap).length);
  for (var family in this.familyMap) {
    Bytes.writeByteArray(out, Bytes.toBytes(family));
    var keys = this.familyMap[family];
    out.writeInt(keys.length);
    for (var j = 0; j < keys.length; j++) {
      var kv = keys[j];
      kv.write(out);
    }
  }
  this.writeAttributes(out);
};

module.exports = Delete;
