/*jslint bitwise: true */
/*!
 * node-hbase-client - lib/data_input_buffer.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');
var DataInputStream = require('./data_input_stream');
var NegativeArraySizeException = require('./errors').NegativeArraySizeException;

function InputBuffer(buf) {
  this.buf = buf;
  this.offset = 0;
}

InputBuffer.prototype.read = function (size) {
  var offset = this.offset;
  var end = offset + size;
  this.offset = end;
  return this.buf.slice(offset, end);
};

function DataInputBuffer(buf) {
  this.in = new InputBuffer(buf);
}
util.inherits(DataInputBuffer, DataInputStream);

/*
 * Read a String as a Network Int n, followed by n Bytes
 * Alternative to 16 bit read/writeUTF.
 * Encoding standard is... ?
 */
DataInputBuffer.prototype.readString = function () {
  var length = this.readInt();
  if (length === -1) {
    return null;
  }
  
  return this.read(length).toString('utf8');
};

/** Read a UTF8 encoded string from in equal to Text.readString()
 */
DataInputBuffer.prototype.readVString = function () {
  var length = this.readVInt();
  return this.read(length).toString('utf8');
};

/**
 * Given the first byte of a vint/vlong, determine the sign
 * 
 * @param value the first byte
 * @return is the value negative
 */
DataInputBuffer.isNegativeVInt = function (value) {
  return value < -120 || (value >= -112 && value < 0);
};

/**
 * Parse the first byte of a vint/vlong to determine the number of bytes
 * 
 * @param value the first byte of the vint/vlong
 * @return the total number of bytes (1 to 9)
 */
DataInputBuffer.decodeVIntSize = function (value) {
  if (value >= -112) {
    return 1;
  } else if (value < -120) {
    return -119 - value;
  }
  return -111 - value;
};

/**
 * Reads a zero-compressed encoded long from input stream and returns it.
 * 
 * @return deserialized long from stream.
 */
DataInputBuffer.prototype.readVLong = function () {
  // TODO: support Long
  var firstByte = this.readByte();
  var len = DataInputBuffer.decodeVIntSize(firstByte);
  if (len === 1) {
    return firstByte;
  }
  var size = len - 1;
  var buf = this.read(size);
  var i = 0;
  for (var idx = 0; idx < size; idx++) {
    var b = buf[idx];
    i = i << 8;
    i = i | (b & 0xFF);
  }
  return DataInputBuffer.isNegativeVInt(firstByte) ? (i ^ -1) : i;
};

/**
 * Reads a zero-compressed encoded integer from input stream and returns it.
 * 
 * @return deserialized integer from stream.
 */
DataInputBuffer.prototype.readVInt = function () {
  return this.readVLong();
};

/**
 * Read byte-array written with a WritableableUtils.vint prefix.
 * 
 * @return byte array read off `this.in`.
 */
DataInputBuffer.prototype.readByteArray = function () {
  var len = this.readVInt();
  return this.in.read(len);
};


module.exports = DataInputBuffer;
