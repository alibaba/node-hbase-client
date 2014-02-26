/*!
 * node-hbase-client - lib/bytes.js
 *
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */


"use strict";

/**
 * Module dependencies.
 */

var utility = require('utility');
var Long = require('long');
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
 * Put an int value out to the specified byte array position.
 * @param bytes the byte array
 * @param offset position in the array
 * @param val int to write out
 * @return incremented offset
 * @throws IllegalArgumentException if the byte array given doesn't have
 * enough room at the offset specified.
 */
exports.putInt = function (bytes, offset, val) {
  bytes.writeInt32BE(val, offset);
  return offset + SIZEOF_INT;
};

/**
 * Put a short value out to the specified byte array position.
 * @param bytes the byte array
 * @param offset position in the array
 * @param val short to write out
 * @return incremented offset
 * @throws IllegalArgumentException if the byte array given doesn't have
 * enough room at the offset specified.
 */
exports.putShort = function (bytes, offset, val) {
  bytes.writeInt16BE(val, offset);
  return offset + SIZEOF_SHORT;
};

/**
 * Put bytes at the specified byte array position.
 * @param bytes the byte array
 * @param offset position in the array
 * @param srcBytes array to write out
 * @return incremented offset
 */
exports.putBytes = function (bytes, offset, srcBytes) {
  srcBytes.copy(bytes, offset);
  return offset + srcBytes.length;
};

/**
 * Write a single byte out to the specified byte array position.
 * @param bytes the byte array
 * @param offset position in the array
 * @param b byte to write out
 * @return incremented offset
 */
exports.putByte = function (bytes, offset, b) {
  bytes[offset] = b;
  return offset + 1;
};

/**
 * Put a long value out to the specified byte array position.
 *
 * @param bytes the byte array
 * @param offset position in the array
 * @param val long to write out
 * @return incremented offset
 * @throws IllegalArgumentException if the byte array given doesn't have
 * enough room at the offset specified.
 */
exports.putLong = function (bytes, offset, val) {
  if (!(val instanceof Long)) {
    val = Long.fromNumber(val);
  }
  bytes.writeInt32BE(val.high, offset);
  bytes.writeInt32BE(val.low, offset + 4);
  return offset + SIZEOF_LONG;
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
  return bytes.readInt32BE(offset);
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
  return bytes.readInt16BE(offset);
};

/**
 * Converts a string to a UTF-8 byte array.
 * @param s string
 * @return the byte array
 */
exports.toBytes = function (s) {
  if (s instanceof Long) {
    return WritableUtils.toLongBytes(s);
  }
  if (Buffer.isBuffer(s)) {
    return s;
  }
  return new Buffer(s, 'utf8');
};

exports.toString = function (b) {
  return b.toString('utf8');
};

/**
 * @param left left operand
 * @param right right operand
 * @return True if equal
 */
exports.equals = function (left, right) {
  // Could use Arrays.equals?
  //noinspection SimplifiableConditionalExpression
  if (left === right) {
    return true;
  }
  if (left === null || right === null) {
    return false;
  }
  if (left.length !== right.length) {
    return false;
  }
  if (left.length === 0) {
    return true;
  }

  // Since we're often comparing adjacent sorted data,
  // it's usual to have equal arrays except for the very last byte
  // so check that first
  if (left[left.length - 1] !== right[right.length - 1]) {
    return false;
  }

  for (var i = 0; i < left.length; i++) {
    if (left[i] !== right[i]) {
      return false;
    }
  }

  return true;
};

/**
 * Write a printable representation of a byte array. Non-printable
 * characters are hex escaped in the format \\x%02X, eg:
 * \x00 \x05 etc
 *
 * @param b array to write out
 * @param off offset to start at
 * @param len length to write
 * @return string output
 */
exports.toStringBinary = function (b, off, len) {
  return b.toString('utf8');
  // off = off || 0;
  // len = len || b.length;
  // var result = '';
  // var first = new String(b, off, len, "ISO-8859-1");
  // for (int i = 0; i < first.length(); ++i) {
  //   int ch = first.charAt(i) & 0xFF;
  //   if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
  //       || " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0) {
  //     result.append(first.charAt(i));
  //   } else {
  //     result.append(String.format("\\x%02X", ch));
  //   }
  // }
};

/**
 * @return 0 if equal, < 0 if left is less than right, etc.
 */
exports.compareTo = function (buffer1, buffer2) {
  var length1 = buffer1.length;
  var length2 = buffer2.length;
  // Short circuit equal case
  if (buffer1 === buffer2 && length1 === length2) {
    return 0;
  }
  // Bring WritableComparator code local
  for (var i = 0, j = 0; i < length1 && j < length2; i++, j++) {
    var a = buffer1[i];
    var b = buffer2[j];
    if (a !== b) {
      return a - b;
    }
  }
  return length1 - length2;
};

/** Compute hash for binary data. */
exports.hashBytes = function (bytes) {
  var hash = 1;
  for (var i = 0; i < bytes.length; i++) {
    hash = (31 * hash) + bytes[i];
  }
  return hash;
};

/**
 * @param b bytes to hash
 * @return A hash of <code>b</code> as an Integer that can be used as key in
 * Maps.
 */
exports.mapKey = function (bytes) {
  return utility.md5(bytes);
};

