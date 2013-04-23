/*!
 * node-hbase-client - lib/bytes.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */


"use strict";

/**
 * Module dependencies.
 */

var WritableUtils = require('../writable_utils');
var errors = require('../errors');
var IllegalArgumentException = errors.IllegalArgumentException;
var NegativeArraySizeException = errors.NegativeArraySizeException;

var SIZEOF_BYTE = exports.SIZEOF_BYTE = 1;
var SIZEOF_BOOLEAN = exports.SIZEOF_BOOLEAN = 1;
var SIZEOF_CHAR = exports.SIZEOF_CHAR = 2;
var SIZEOF_SHORT = exports.SIZEOF_SHORT = 2;
var SIZEOF_INT = exports.SIZEOF_INT = 4;
var SIZEOF_DOUBLE = exports.SIZEOF_DOUBLE = 8;
var SIZEOF_LONG = exports.SIZEOF_LONG = 8;

/**
 * Write byte-array to out with a vint length prefix.
 * @param out output stream
 * @param b array
 * @param offset offset into array
 * @param length length past offset
 */
exports.writeByteArray = function (out, b, offset, length) {
  length = length || (b && b.length) || 0;
  offset = offset || 0;
  WritableUtils.writeVInt(out, length);
  if (length > 0) {
    out.write(b, offset, length);
  }
};

/**
 * Read byte-array written with a WritableableUtils.vint prefix.
 * @param in Input to read from.
 * @return byte array read off <code>in</code>
 * @throws IOException e
 */
exports.readByteArray = function (io, callback) {
  WritableUtils.readVInt(io, function (err, len) {
    if (err) {
      return callback(err);
    }
    if (len < 0) {
      return callback(new NegativeArraySizeException(len + ' must be >= 0'));
    }
    io.readFully(len, callback);
  });
};

/**
 * Put an int value out to the specified byte array position.
 * @param bytes the byte array
 * @param offset position in the array
 * @param val int to write out
 * @return incremented offset
 * @throws IllegalArgumentException if the byte array given doesn't have
 * enough room at the offset specified.
 */
exports.putInt = function (bytes, offset, val) {
  if (bytes.length - offset < SIZEOF_INT) {
    throw new IllegalArgumentException("Not enough room to put an int at" + " offset " + 
      offset + " in a " + bytes.length + " byte array");
  }
  bytes.writeInt32BE(val, offset);
  return offset + SIZEOF_INT;
};

/**
 * Converts a byte array to an int value
 * @param bytes byte array
 * @param offset offset into array
 * @param length length of int (has to be {@link #SIZEOF_INT})
 * @return the int value
 * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
 * if there's not enough room in the array at the offset indicated.
 */
exports.toInt = function (bytes, offset, length) {
  length = length || SIZEOF_INT;
  var len = offset + length;
  if (length !== SIZEOF_INT || len > bytes.length) {
    throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
  }
  return bytes.readInt32BE(offset);
  // var n = 0;
  // for (var i = offset; i < len; i++) {
  //   n <<= 8;
  //   n ^= bytes[i] & 0xFF;
  // }
  // return n;
};

/**
 * Converts a byte array to a short value
 * @param bytes byte array
 * @param offset offset into array
 * @param length length, has to be {@link #SIZEOF_SHORT}
 * @return the short value
 * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
 * or if there's not enough room in the array at the offset indicated.
 */
exports.toShort = function (bytes, offset, length) {
  length = length || SIZEOF_SHORT;
  if (length !== SIZEOF_SHORT || offset + length > bytes.length) {
    throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
  }
  return bytes.readInt16BE(offset);
};

/**
 * Converts a string to a UTF-8 byte array.
 * @param s string
 * @return the byte array
 */
exports.toBytes = function (s) {
  return new Buffer(s, 'utf8');
};

exports.toString = function (b) {
  return b.toString('utf8');
};
