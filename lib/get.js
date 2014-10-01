/*!
 * node-hbase-client - lib/get.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var HbaseObjectWritable = require('./io/hbase_object_writable');
var eventproxy = require('eventproxy');
var IOException = require('./errors').IOException;
var util = require('util');
var Bytes = require('./util/bytes');
var TimeRange = require('./time_range');
var OperationWithAttributes = require('./operation_with_attributes');

var GET_VERSION = 2;

function Get(row, rowLock) {
  if (!(this instanceof Get)) {
    return new Get(row, rowLock);
  }

  OperationWithAttributes.call(this);

  if (row && !Buffer.isBuffer(row)) {
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
  this.familyMap = {};
}

util.inherits(Get, OperationWithAttributes);

/**
 * Get all columns from the specified family.
 * <p>
 * Overrides previous calls to addColumn for this family.
 *
 * @param family family name
 * @return the Get object
 */
Get.prototype.addFamily = function (family) {
  delete this.familyMap[family];
  this.familyMap[family] = null;
  return this;
};

/**
 * Get the column from the specific family with the specified qualifier.
 * <p>
 * Overrides previous calls to addFamily for this family.
 *
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
 * Get up to the specified number of versions of each column.
 *
 * @param maxVersions maximum versions for each column
 * @return this for invocation chaining
 */
Get.prototype.setMaxVersions = function (maxVersions) {
  if (maxVersions <= 0) {
    maxVersions = 1;
  }
  this.maxVersions = maxVersions;
  return this;
};

/**
 * Get versions of columns only within the specified timestamp range,
 * [minStamp, maxStamp).
 *
 * @param minStamp minimum timestamp value, inclusive
 * @param maxStamp maximum timestamp value, exclusive
 * @return this for invocation chaining
 */
Get.prototype.setTimeRange = function (minStamp, maxStamp) {
  this.tr = new TimeRange(minStamp, maxStamp);
  return this;
};

Get.prototype.readFields = function (io) {
  var version = io.readByte();
  if (version > GET_VERSION) {
    throw new IOException("unsupported version: " + version);
  }
  this.version = version;
  this.row = io.readByteArray();
  this.lockId = io.readLong();
  this.maxVersions = io.readInt();
  var hasFilter = io.readBoolean();
  if (hasFilter) {
    this.filter.readFields(io);
  }
  this.hasFilter = hasFilter;
  this.cacheBlocks = io.readBoolean();
  this.tr = new TimeRange();
  this.tr.readFields(io);
  var familyMap = {};
  var num = io.readInt();
  for (var i = 0; i < num; i++) {
    var family = Bytes.toString(io.readByteArray());
    familyMap[family] = null;
    var hasColumns = io.readBoolean();
    if (hasColumns) {
      var set = [];
      var columnNum = io.readInt();
      for (var j = 0; j < columnNum; j++) {
        var qualifier = io.readByteArray();
        set.push(qualifier);
      }
      familyMap[family] = set;
    }
  }
  this.familyMap = familyMap;
  this.readAttributes(io);
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

Get.prototype.getRow = function () {
  return this.row;
};


HbaseObjectWritable.addToClass('Get.class', Get);
module.exports = Get;
