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

/*
 *
 * Write a String as a Network Int n, followed by n Bytes
 * Alternative to 16 bit read/writeUTF.
 * Encoding standard is... ?
 * 
 */
exports.writeString = function (out, s) {
  if (s !== null || s !== undefined) {
    var buffer = s.getBytes("UTF-8");
    var len = buffer.length;
    out.writeInt(len);
    out.write(buffer, 0, len);
  } else {
    out.writeInt(-1);
  }
};

/*
 * Read a String as a Network Int n, followed by n Bytes
 * Alternative to 16 bit read/writeUTF.
 * Encoding standard is... ?
 *
 */
exports.readString = function (io) {
  var length = io.readInt();
  if (length === -1) {
    return null;
  }
  var buffer = new byte[length];
  io.readFully(buffer); // could/should use readFully(buffer,0,length)?
  return new String(buffer, "UTF-8");
};

