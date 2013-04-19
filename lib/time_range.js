/*!
 * node-hbase-client - lib/time_range.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Bytes = require('./bytes');

var MAX_VALUE = Math.pow(2, 53);

var MAX_VALUE_BYTES = new Buffer([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

/**
 * Represents an interval of version timestamps.
 * <p>
 * Evaluated according to minStamp <= timestamp < maxStamp
 * or [minStamp,maxStamp) in interval notation.
 * <p>
 * Only used internally; should not be accessed directly by clients.
 */
function TimeRange(minStamp, maxStamp, allTime) {
  if (Buffer.isBuffer(minStamp)) {
    minStamp = Bytes.toLong(minStamp);
  }
  if (Buffer.isBuffer(maxStamp)) {
    maxStamp = Bytes.toLong(maxStamp);
  }
  if (maxStamp < minStamp) {
    var err = new Error("maxStamp is smaller than minStamp");
    err.name = 'IOException';
    throw err;
  }
  this.minStamp = minStamp || 0;
  this.maxStamp = maxStamp || MAX_VALUE; // Long.MAX_VALUE;
  if (allTime === undefined || allTime === null) {
    allTime = true;
  }
  this.allTime = allTime;
}

  // /**
  //  * Default constructor.
  //  * Represents interval [0, Long.MAX_VALUE) (allTime)
  //  */
  // public TimeRange() {
  //   allTime = true;
  // }

  // /**
  //  * Represents interval [minStamp, Long.MAX_VALUE)
  //  * @param minStamp the minimum timestamp value, inclusive
  //  */
  // public TimeRange(long minStamp) {
  //   this.minStamp = minStamp;
  // }

  // /**
  //  * Represents interval [minStamp, Long.MAX_VALUE)
  //  * @param minStamp the minimum timestamp value, inclusive
  //  */
  // public TimeRange(byte[] minStamp) {
  //   this.minStamp = Bytes.toLong(minStamp);
  // }

  // *
  //  * Represents interval [minStamp, maxStamp)
  //  * @param minStamp the minimum timestamp, inclusive
  //  * @param maxStamp the maximum timestamp, exclusive
  //  * @throws IOException
   
  // public TimeRange(long minStamp, long maxStamp) throws IOException {
  //   if (maxStamp < minStamp) {
  //     throw new IOException("maxStamp is smaller than minStamp");
  //   }
  //   this.minStamp = minStamp;
  //   this.maxStamp = maxStamp;
  // }

  // /**
  //  * Represents interval [minStamp, maxStamp)
  //  * @param minStamp the minimum timestamp, inclusive
  //  * @param maxStamp the maximum timestamp, exclusive
  //  * @throws IOException
  //  */
  // public TimeRange(byte[] minStamp, byte[] maxStamp) throws IOException {
  //   this(Bytes.toLong(minStamp), Bytes.toLong(maxStamp));
  // }

/**
 * @return the smallest timestamp that should be considered
 */
TimeRange.prototype.getMin = function () {
  return this.minStamp;
};

/**
 * @return the biggest timestamp that should be considered
 */
TimeRange.prototype.getMax = function () {
  return this.maxStamp;
};

/**
 * Check if the specified timestamp is within this TimeRange.
 * <p>
 * Returns true if within interval [minStamp, maxStamp), false
 * if not.
 * @param timestamp timestamp to check
 * @param [offset] offset into the bytes
 * @return true if within TimeRange, false if not
 */
TimeRange.prototype.withinTimeRange = function (timestamp, offset) {
  if (this.allTime) {
    return true;
  }
  if (Buffer.isBuffer(timestamp)) {
    timestamp = Bytes.toLong(timestamp, offset);
  }
  // check if >= minStamp
  return (this.minStamp <= timestamp && timestamp < this.maxStamp);
};

/**
 * Check if the specified timestamp is within this TimeRange.
 * <p>
 * Returns true if within interval [minStamp, maxStamp), false
 * if not.
 * @param timestamp timestamp to check
 * @return true if within TimeRange, false if not
 */
TimeRange.prototype.withinOrAfterTimeRange = function (timestamp) {
  if (this.allTime) {
    return true;
  }
  // check if >= minStamp
  return timestamp >= this.minStamp;
};

/**
 * Compare the timestamp to timerange
 * @param timestamp
 * @return -1 if timestamp is less than timerange,
 * 0 if timestamp is within timerange,
 * 1 if timestamp is greater than timerange
 */
TimeRange.prototype.compare = function (timestamp) {
  if (timestamp < this.minStamp) {
    return -1;
  } else if (timestamp >= this.maxStamp) {
    return 1;
  } else {
    return 0;
  }
};

TimeRange.prototype.toString = function () {
  return "maxStamp=" + this.maxStamp + ", minStamp=" + this.minStamp;
};

//Writable
TimeRange.prototype.readFields = function (io) {
  this.minStamp = io.readLong();
  this.maxStamp = io.readLong();
  this.allTime = io.readBoolean();
};

TimeRange.prototype.write = function (out) {
  out.writeLong(this.minStamp);
  if (this.maxStamp === MAX_VALUE) {
    out.write(MAX_VALUE_BYTES);
  } else {
    out.writeLong(this.maxStamp);
  }
  out.writeBoolean(this.allTime);
};

module.exports = TimeRange;
