/*!
 * node-hbase-client - lib/time_range.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Long = require('long');
var Bytes = require('./util/bytes');

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
  
  this.minStamp = minStamp || 0;
  this.maxStamp = maxStamp || Long.MAX_VALUE;
  if (allTime === undefined || allTime === null) {
    allTime = true;
  }
  this.allTime = allTime;
}

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
  return "maxStamp=" + this.maxStamp.toString() + ", minStamp=" + this.minStamp.toString();
};

//Writable
TimeRange.prototype.readFields = function (io) {
  this.minStamp = io.readLong();
  this.maxStamp = io.readLong();
  this.allTime = io.readBoolean();
};

TimeRange.prototype.write = function (out) {
  out.writeLong(this.minStamp);
  out.writeLong(this.maxStamp);
  out.writeBoolean(this.allTime);
};


module.exports = TimeRange;
