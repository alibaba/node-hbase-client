/*jslint bitwise: true */
/*!
 * node-hbase-client - lib/writable_utils.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Long = require('long');

/**
 * Serializes a long to a binary stream with zero-compressed encoding.
 * For -112 <= i <= 127, only one byte is used with the actual value.
 * 
 * For other values of i, the first byte value indicates whether the
 * long is positive or negative, and the number of bytes that follow.
 * If the first byte value v is between -113 and -120, the following long
 * is positive, with number of bytes that follow are -(v+112).
 * If the first byte value v is between -121 and -128, the following long
 * is negative, with number of bytes that follow are -(v+120). Bytes are
 * stored in the high-non-zero-byte-first order.
 * 
 * @param stream Binary output stream
 * @param i Long to be serialized
 */
exports.writeVLong = function (stream, i) {
  // TODO: support Long
  if (i >= -112 && i <= 127) {
    stream.writeByte(i);
    return;
  }

  var len = -112;
  if (i < 0) {
    // i ^= -1L; // take one's complement'
    i ^= -1; // take one's complement'
    len = -120;
  }

  var tmp = i;
  while (tmp !== 0) {
    tmp = tmp >> 8;
    len--;
  }

  // stream.writeByte(len);

  var left = (len < -120) ? -(len + 120) : -(len + 112);
  var buf = new Buffer(left + 1);
  buf[0] = len;

  for (var idx = left, bi = 1; idx !== 0; idx--, bi++) {
    var shiftbits = (idx - 1) * 8;
    // var mask = 0xFFL << shiftbits;
    var mask = 0xFF << shiftbits;
    buf[bi] = (i & mask) >> shiftbits;
  }

  stream.writeBytes(buf);
};

/**
 * Serializes an integer to a binary stream with zero-compressed encoding.
 * For -120 <= i <= 127, only one byte is used with the actual value.
 * For other values of i, the first byte value indicates whether the
 * integer is positive or negative, and the number of bytes that follow.
 * If the first byte value v is between -121 and -124, the following integer
 * is positive, with number of bytes that follow are -(v+120).
 * If the first byte value v is between -125 and -128, the following integer
 * is negative, with number of bytes that follow are -(v+124). Bytes are
 * stored in the high-non-zero-byte-first order.
 *
 * @param stream Binary output stream
 * @param i Integer to be serialized
 * @throws java.io.IOException 
 */
exports.writeVInt = function (stream, i) {
  exports.writeVLong(stream, i);
};

/**
 * Reads a zero-compressed encoded long from input stream and returns it.
 * @param stream Binary input stream
 * @throws java.io.IOException 
 * @return deserialized long from stream.
 */
exports.readVLong = function (stream, callback) {
  // TODO: support Long
  stream.readFields([{name: 'firstByte', method: 'readByte'}], function (err, data) {
    if (err) {
      return callback(err);
    }
    var firstByte = data.firstByte;
    var len = exports.decodeVIntSize(firstByte);
    if (len === 1) {
      return callback(null, firstByte);
    }
    var size = len - 1;
    stream.readFully(size, function (err, buf) {
      if (err) {
        return callback(err);
      }
      var i = 0;
      for (var idx = 0; idx < size; idx++) {
        var b = buf[idx];
        i = i << 8;
        i = i | (b & 0xFF);
      }
      callback(null, exports.isNegativeVInt(firstByte) ? (i ^ -1) : i);
    });
  });
};

/**
 * Reads a zero-compressed encoded integer from input stream and returns it.
 * @param stream Binary input stream
 * @throws java.io.IOException 
 * @return deserialized integer from stream.
 */
exports.readVInt = function (stream, callback) {
  exports.readVLong(stream, callback);
};

/**
 * Given the first byte of a vint/vlong, determine the sign
 * @param value the first byte
 * @return is the value negative
 */
exports.isNegativeVInt = function (value) {
  return value < -120 || (value >= -112 && value < 0);
};

/**
 * Parse the first byte of a vint/vlong to determine the number of bytes
 * @param value the first byte of the vint/vlong
 * @return the total number of bytes (1 to 9)
 */
exports.decodeVIntSize = function (value) {
  if (value >= -112) {
    return 1;
  } else if (value < -120) {
    return -119 - value;
  }
  return -111 - value;
};

/**
 * Convert v to Long.
 * 
 * @param {Number|String} v
 * @return {Long}
 */
exports.toLong = function (v) {
  if (v instanceof Long) {
    return v;
  }
  if (Buffer.isBuffer(v)) {
    // buffer must be 8 bytes
    return Long.fromBits(v.readInt32BE(4), v.readInt32BE(0));
  }
  if (typeof v === 'string') {
    return Long.fromString(v);
  }
  return Long.fromNumber(v);
};

exports.toLongBytes = function (v) {
  var buf = new Buffer(8);
  var longV = exports.toLong(v);
  buf.writeInt32BE(longV.high, 0);
  buf.writeInt32BE(longV.low, 4);
  return buf;
};

