/*!
 * node-hbase-client - lib/bytes.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */


"use strict";

/**
 * Module dependencies.
 */

/**
 * Write byte-array to out with a vint length prefix.
 * @param out output stream
 * @param b array
 * @param offset offset into array
 * @param length length past offset
 */
exports.writeByteArray = function (out, b, offset, length) {
  if (!b || b.length === 0 || length === 0) {
    out.writeByte()
  }
  WritableUtils.writeVInt(out, length);
  out.write(b, offset, length);
}