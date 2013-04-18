/*!
 * node-hbase-client - lib/writable_utils.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

/**
 * Serializes a long to a binary stream with zero-compressed encoding.
 * For -112 <= i <= 127, only one byte is used with the actual value.
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
  if (i >= -112 && i <= 127) {
    stream.writeByte((byte) i);
    return;
  }

  int len = -112;
  if (i < 0) {
    i ^= -1L; // take one's complement'
    len = -120;
  }

  long tmp = i;
  while (tmp != 0) {
    tmp = tmp >> 8;
    len--;
  }

  stream.writeByte((byte) len);

  len = (len < -120) ? -(len + 120) : -(len + 112);

  for (int idx = len; idx != 0; idx--) {
    int shiftbits = (idx - 1) * 8;
    long mask = 0xFFL << shiftbits;
    stream.writeByte((byte) ((i & mask) >> shiftbits));
  }
}
