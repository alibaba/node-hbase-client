/*!
 * node-hbase-client - lib/scan.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');
var OperationWithAttributes = require('./operation_with_attributes');
var Bytes = require('./util/bytes');
var HConstants = require('./hconstants');
var TimeRange = require('./time_range');

var RAW_ATTR = "_raw_";
var ISOLATION_LEVEL = "_isolationlevel_";
var SCAN_VERSION = 3;
// If application wants to collect scan metrics, it needs to
// call scan.setAttribute(SCAN_ATTRIBUTES_ENABLE, Bytes.toBytes(Boolean.TRUE))
var SCAN_ATTRIBUTES_METRICS_ENABLE = "scan.attributes.metrics.enable";
var SCAN_ATTRIBUTES_METRICS_DATA = "scan.attributes.metrics.data";

/**
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of
 * instantiation.  Rather than specifying a single row, an optional startRow
 * and stopRow may be defined.  If rows are not specified, the Scanner will
 * iterate over all rows.
 * <p>
 * To scan everything for each row, instantiate a Scan object.
 * <p>
 * To modify scanner caching for just this scan, use {@link #setCaching(int) setCaching}.
 * If caching is NOT set, we will use the caching value of the hosting {@link HTable}.  See
 * {@link HTable#setScannerCaching(int)}. In addition to row caching, it is possible to specify a
 * maximum result size, using {@link #setMaxResultSize(long)}. When both are used,
 * single server requests are limited by either number of rows or maximum result size, whichever
 * limit comes first.
 * <p>
 * To further define the scope of what to get when scanning, perform additional
 * methods as outlined below.
 * <p>
 * To get all columns from specific families, execute {@link #addFamily(byte[]) addFamily}
 * for each family to retrieve.
 * <p>
 * To get specific columns, execute {@link #addColumn(byte[], byte[]) addColumn}
 * for each column to retrieve.
 * <p>
 * To only retrieve columns within a specific range of version timestamps,
 * execute {@link #setTimeRange(long, long) setTimeRange}.
 * <p>
 * To only retrieve columns with a specific timestamp, execute
 * {@link #setTimeStamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To limit the maximum number of values returned for each call to next(),
 * execute {@link #setBatch(int) setBatch}.
 * <p>
 * To add a filter, execute {@link #setFilter(org.apache.hadoop.hbase.filter.Filter) setFilter}.
 * <p>
 * Expert: To explicitly disable server-side block caching for this scan,
 * execute {@link #setCacheBlocks(boolean)}.
 */
function Scan(startRow, stopRow) {
  OperationWithAttributes.call(this);
  
  this.startRow = startRow || HConstants.EMPTY_START_ROW;
  this.stopRow = stopRow || HConstants.EMPTY_END_ROW;
  this.maxVersions = 1;
  this.batch = -1;

  /*
   * -1 means no caching
   */
  this.caching = -1;
  this.maxResultSize = -1;
  this.cacheBlocks = true;
  this.filter = null;
  this.tr = new TimeRange();
  this.familyMap = {};
}

util.inherits(Scan, OperationWithAttributes);

Scan.prototype.getRow = function () {
  return this.startRow;
};

/**
 * Get all columns from the specified family.
 * <p>
 * Overrides previous calls to addColumn for this family.
 * @param family family name
 * @return this
 */
Scan.prototype.addFamily = function (family) {
  this.familyMap[family] = null;
  return this;
};

/**
 * Get the column from the specified family with the specified qualifier.
 * <p>
 * Overrides previous calls to addFamily for this family.
 * @param family family name
 * @param qualifier column qualifier
 * @return this
 */
Scan.prototype.addColumn = function (family, qualifier) {
  var set = this.familyMap[family];
  if (!set) {
    this.familyMap[family] = set = [];
  }
  set.push(qualifier);
  return this;
};

/**
 * Get versions of columns only within the specified timestamp range,
 * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
 * your time range spans more than one version and you want all versions
 * returned, up the number of versions beyond the defaut.
 * @param minStamp minimum timestamp value, inclusive
 * @param maxStamp maximum timestamp value, exclusive
 * @throws IOException if invalid time range
 * @see #setMaxVersions()
 * @see #setMaxVersions(int)
 * @return this
 */
Scan.prototype.setTimeRange = function (minStamp, maxStamp) {
  this.tr = new TimeRange(minStamp, maxStamp);
  return this;
};

/**
 * Get versions of columns with the specified timestamp. Note, default maximum
 * versions to return is 1.  If your time range spans more than one version
 * and you want all versions returned, up the number of versions beyond the
 * defaut.
 * @param timestamp version timestamp
 * @see #setMaxVersions()
 * @see #setMaxVersions(int)
 * @return this
 */
Scan.prototype.setTimeStamp = function (timestamp) {
  this.tr = new TimeRange(timestamp, timestamp + 1);
  return this;
};

/**
 * Set the maximum number of values to return for each call to next()
 * @param batch the maximum number of values
 */
Scan.prototype.setBatch = function (batch) {
  if (this.hasFilter() && this.filter.hasFilterRow()) {
    throw new IncompatibleFilterException("Cannot set batch on a scan using a filter"
        + " that returns true for filter.hasFilterRow");
  }
  this.batch = batch;
};

//Writable
// public void readFields(final DataInput in) throws IOException {
//   int version = in.readByte();
//   if (version > (int) SCAN_VERSION) {
//     throw new IOException("version not supported");
//   }
//   this.startRow = Bytes.readByteArray(in);
//   this.stopRow = Bytes.readByteArray(in);
//   this.maxVersions = in.readInt();
//   this.batch = in.readInt();
//   this.caching = in.readInt();
//   this.cacheBlocks = in.readBoolean();
//   if (in.readBoolean()) {
//     this.filter = (Filter) createForName(Bytes.toString(Bytes.readByteArray(in)));
//     this.filter.readFields(in);
//   }
//   this.tr = new TimeRange();
//   tr.readFields(in);
//   int numFamilies = in.readInt();
//   this.familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
//   for (int i = 0; i < numFamilies; i++) {
//     byte[] family = Bytes.readByteArray(in);
//     int numColumns = in.readInt();
//     TreeSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
//     for (int j = 0; j < numColumns; j++) {
//       byte[] qualifier = Bytes.readByteArray(in);
//       set.add(qualifier);
//     }
//     this.familyMap.put(family, set);
//   }

//   if (version > 1) {
//     readAttributes(in);
//   }
//   if (version > 2) {
//     this.maxResultSize = in.readLong();
//   }
// }

Scan.prototype.write = function (out) {
  out.writeByte(SCAN_VERSION);
  Bytes.writeByteArray(out, this.startRow);
  Bytes.writeByteArray(out, this.stopRow);
  out.writeInt(this.maxVersions);
  out.writeInt(this.batch);
  out.writeInt(this.caching);
  out.writeBoolean(this.cacheBlocks);
  if (!this.filter) {
    out.writeBoolean(false);
  } else {
    out.writeBoolean(true);
    Bytes.writeByteArray(out, Bytes.toBytes(this.filter.getClass().getName()));
    this.filter.write(out);
  }
  this.tr.write(out);
  out.writeInt(Object.keys(this.familyMap).length);
  for (var family in this.familyMap) {
    Bytes.writeByteArray(out, Bytes.toBytes(family));
    var columnSet = this.familyMap[family];
    if (columnSet && columnSet.length > 0) {
      out.writeInt(columnSet.length);
      for (var i = 0; i < columnSet.length; i++) {
        var qualifier = columnSet[i];
        Bytes.writeByteArray(out, qualifier);
      }
    } else {
      out.writeInt(0);
    }
  }
  this.writeAttributes(out);
  out.writeLong(this.maxResultSize);
};

/**
 * Enable/disable "raw" mode for this scan.
 * If "raw" is enabled the scan will return all
 * delete marker and deleted rows that have not
 * been collected, yet.
 * This is mostly useful for Scan on column families
 * that have KEEP_DELETED_ROWS enabled.
 * It is an error to specify any column when "raw" is set.
 * @param raw True/False to enable/disable "raw" mode.
 */
Scan.prototype.setRaw = function (raw) {
  setAttribute(RAW_ATTR, Bytes.toBytes(raw));
}

/**
 * @return True if this Scan is in "raw" mode.
 */
Scan.prototype.isRaw = function () {
  var attr = this.getAttribute(RAW_ATTR);
  return attr == null ? false : Bytes.toBoolean(attr);
}

/*
 * Set the isolation level for this scan. If the
 * isolation level is set to READ_UNCOMMITTED, then
 * this scan will return data from committed and
 * uncommitted transactions. If the isolation level 
 * is set to READ_COMMITTED, then this scan will return 
 * data from committed transactions only. If a isolation
 * level is not explicitly set on a Scan, then it 
 * is assumed to be READ_COMMITTED.
 * @param level IsolationLevel for this scan
 */
Scan.prototype.setIsolationLevel = function (level) {
  this.setAttribute(ISOLATION_LEVEL, level.toBytes());
}

/*
 * @return The isolation level of this scan.
 * If no isolation level was set for this scan object, 
 * then it returns READ_COMMITTED.
 * @return The IsolationLevel for this scan
 */
Scan.prototype.getIsolationLevel = function () {
  var attr = this.getAttribute(ISOLATION_LEVEL);
  return attr == null ? IsolationLevel.READ_COMMITTED : IsolationLevel.fromBytes(attr);
}


module.exports = Scan;
