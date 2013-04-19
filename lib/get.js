/*!
 * node-hbase-client - lib/get.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');
var Bytes = require('./bytes');
var TimeRange = require('./time_range');
var OperationWithAttributes = require('./operation_with_attributes');

var GET_VERSION = 2;

function Get(row, rowLock) {
  OperationWithAttributes.call(this);

  if (!Buffer.isBuffer(row)) {
    row = Bytes.toBytes(row);
  }

  this.row = row;
  this.lockId = -1;
  if (rowLock) {
    this.lockId = rowLock.getLockId();
  }
  this.maxVersions = 1;
  this.cacheBlocks = true;
  this.filter = null;
  this.tr = new TimeRange();
  this.familyMap = {}; // new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
}

util.inherits(Get, OperationWithAttributes);

/**
 * Get all columns from the specified family.
 * <p>
 * Overrides previous calls to addColumn for this family.
 * @param family family name
 * @return the Get object
 */
Get.prototype.addFamily = function (family) {
  delete this.familyMap[family];
  this.familyMap[family] = null;
  // familyMap.remove(family);
  // familyMap.put(family, null);
  return this;
};

/**
 * Get the column from the specific family with the specified qualifier.
 * <p>
 * Overrides previous calls to addFamily for this family.
 * @param family family name
 * @param qualifier column qualifier
 * @return the Get objec
 */
Get.prototype.addColumn = function (family, qualifier) {
  var set = this.familyMap[family] || [];
  set.push(qualifier);
  this.familyMap[family] = set;
  // NavigableSet<byte[]> set = familyMap.get(family);
  // if (set == null) {
  //   set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  // }
  // set.add(qualifier);
  // familyMap.put(family, set);
  return this;
};

/**
 * Get versions of columns only within the specified timestamp range,
 * [minStamp, maxStamp).
 * @param minStamp minimum timestamp value, inclusive
 * @param maxStamp maximum timestamp value, exclusive
 * @throws IOException if invalid time range
 * @return this for invocation chaining
 */
Get.prototype.setTimeRange = function (minStamp, maxStamp) {
  this.tr = new TimeRange(minStamp, maxStamp);
  return this;
};

Get.prototype.write = function (out) {
  out.writeByte(GET_VERSION);
  Bytes.writeByteArray(out, this.row);
  out.writeLong(this.lockId);
  out.writeInt(this.maxVersions);
  if (this.filter === null) {
    out.writeBoolean(false);
  } else {
    out.writeBoolean(true);
    Bytes.writeByteArray(out, Bytes.toBytes(this.filter.getClass().getName()));
    this.filter.write(out);
  }
  out.writeBoolean(this.cacheBlocks);
  this.tr.write(out);
  // out.writeInt(familyMap.size());
  out.writeInt(Object.keys(this.familyMap).length);
  for (var family in this.familyMap) {
    Bytes.writeByteArray(out, Bytes.toBytes(family));
    var columnSet = this.familyMap[family];
    if (columnSet === null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(columnSet.length);
      for (var i = 0; i < columnSet.length; i++) {
        var qualifier = columnSet[i];
        Bytes.writeByteArray(out, Bytes.toBytes(qualifier));
      }
    }
  }
  this.writeAttributes(out);
};

module.exports = Get;
