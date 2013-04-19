/*!
 * node-hbase-client - lib/bytes.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */


"use strict";

/**
 * Module dependencies.
 */

var WritableUtils = require('./writable_utils');

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
 * Converts a string to a UTF-8 byte array.
 * @param s string
 * @return the byte array
 */
exports.toBytes = function (s) {
  return new Buffer(s, 'utf8');
};
