/*!
 * node-hbase-client - lib/out_stream.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

function OutStream(out) {
  this.out = out;
}

/**
 * Writes out a <code>byte</code> to the underlying output stream as 
 * a 1-byte value. If no exception is thrown, the counter 
 * <code>written</code> is incremented by <code>1</code>.
 *
 * @param      v   a <code>byte</code> value to be written.
 * @exception  IOException  if an I/O error occurs.
 * @see        java.io.FilterOutputStream#out
 */
OutStream.prototype.writeByte = function (v) {
  if (!Buffer.isBuffer(v)) {
    v = new Buffer([v]);
  }
  this.out.write(v);
};

/**
 * Writes a <code>boolean</code> to the underlying output stream as 
 * a 1-byte value. The value <code>true</code> is written out as the 
 * value <code>(byte)1</code>; the value <code>false</code> is 
 * written out as the value <code>(byte)0</code>. If no exception is 
 * thrown, the counter <code>written</code> is incremented by 
 * <code>1</code>.
 *
 * @param      v   a <code>boolean</code> value to be written.
 * @exception  IOException  if an I/O error occurs.
 * @see        java.io.FilterOutputStream#out
 */
OutStream.prototype.writeBoolean = function (v) {
  this.writeByte(v ? 1 : 0);
};

/**
 * Writes a <code>short</code> to the underlying output stream as two
 * bytes, high byte first. If no exception is thrown, the counter 
 * <code>written</code> is incremented by <code>2</code>.
 *
 * @param      v   a <code>short</code> to be written.
 * @exception  IOException  if an I/O error occurs.
 * @see        java.io.FilterOutputStream#out
 */
OutStream.prototype.writeShort = function (v) {
  this.writeChar(v);
};

/**
 * Writes a <code>char</code> to the underlying output stream as a 
 * 2-byte value, high byte first. If no exception is thrown, the 
 * counter <code>written</code> is incremented by <code>2</code>.
 *
 * @param      v   a <code>char</code> value to be written.
 * @exception  IOException  if an I/O error occurs.
 * @see        java.io.FilterOutputStream#out
 */
OutStream.prototype.writeChar = function (v) {
  var buf = new Buffer(2);
  buf[0] = v >>> 8;
  buf[1] = v >>> 0;
  this.out.write(buf);
};

/**
 * Writes an <code>int</code> to the underlying output stream as four
 * bytes, high byte first. If no exception is thrown, the counter 
 * <code>written</code> is incremented by <code>4</code>.
 *
 * @param      v   an <code>int</code> to be written.
 * @exception  IOException  if an I/O error occurs.
 * @see        java.io.FilterOutputStream#out
 */
OutStream.prototype.writeInt = function (v) {
  var buf = new Buffer(4);
  buf[0] = v >>> 24;
  buf[1] = v >>> 16;
  buf[2] = v >>> 8;
  buf[3] = v >>> 0;
  this.out.write(buf);
};

var ZERO_LONG_BUFFER = new Buffer([0, 0, 0, 0, 0, 0, 0, 0]);

var MAX_INT32 = 4294967295;
var MIN_INT32 = -4294967296;
// var MAX_NUMBER = 9007199254740992;

/**
 * Writes a <code>long</code> to the underlying output stream as eight
 * bytes, high byte first. In no exception is thrown, the counter 
 * <code>written</code> is incremented by <code>8</code>.
 *
 * @param      v   a <code>long</code> to be written.
 * @exception  IOException  if an I/O error occurs.
 * @see        java.io.FilterOutputStream#out
 */
OutStream.prototype.writeLong = function (v) {
  // In Javascript, numbers are 64 bit floating point values. 
  // The largest integer (magnitude) is 253, or Math.pow(2,53), or 9007199254740992.
  // Bitwise Operators: https://developer.mozilla.org/en-US/docs/JavaScript/Reference/Operators/Bitwise_Operators
  // v should below MAX_INT32.
  //
  // 32位带符号整数表单范围是 -Math.pow(2,31) ~ Math.pow(2,31)-1 即 -2147483648～2147483647,
  // 而 js 数字的精度是双精度，64位，如果一个超过 2147483647 的整数参与位运算的时候就需要注意，
  // 其二进制溢出了,截取32位后，如果第32位是1将被解读为负数(补码)。
  // 
  // 位移运算不能移动超过32位，如果试图移动超过31位，将位数 对32取模后再移位
  
  // java code
  // writeBuffer[0] = v >>> 56;
  // writeBuffer[1] = v >>> 48;
  // writeBuffer[2] = v >>> 40;
  // writeBuffer[3] = v >>> 32;
  // writeBuffer[4] = v >>> 24;
  // writeBuffer[5] = v >>> 16;
  // writeBuffer[6] = v >>>  8;
  // writeBuffer[7] = v >>>  0;

  if (v === 0) {
    this.out.write(ZERO_LONG_BUFFER);
    return;
  }

  var writeBuffer = new Buffer(8);
  if (v > 0) {
    if (v <= MAX_INT32) {
      // 高位补0
      writeBuffer[0] = 0;
      writeBuffer[1] = 0;
      writeBuffer[2] = 0;
      writeBuffer[3] = 0;
    } else {
      var high = Math.floor(v / MAX_INT32);
      writeBuffer[0] = high >>> 24;
      writeBuffer[1] = high >>> 16;
      writeBuffer[2] = high >>> 8;
      writeBuffer[3] = high >>> 0;
    }
  } else {
    if (v >= MIN_INT32) {
      writeBuffer[0] = 255;
      writeBuffer[1] = 255;
      writeBuffer[2] = 255;
      writeBuffer[3] = 255;
    } else {
      var high = Math.floor(v / MAX_INT32);
      writeBuffer[0] = high >>> 24;
      writeBuffer[1] = high >>> 16;
      writeBuffer[2] = high >>> 8;
      writeBuffer[3] = high >>> 0;
    }
  }
  writeBuffer[4] = v >>> 24;
  writeBuffer[5] = v >>> 16;
  writeBuffer[6] = v >>>  8;
  writeBuffer[7] = v >>>  0;
  this.out.write(writeBuffer);
};

module.exports = OutStream;
