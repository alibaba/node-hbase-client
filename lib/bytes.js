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
      throw new NegativeArraySizeException(Integer.toString(len));
    }
    io.readFully(len, callback);
  });
},

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
