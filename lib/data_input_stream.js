/*!
 * node-hbase-client - lib/data_input_stream.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase:data_input_stream');
var Readable = require('readable-stream').Readable;
var Bytes = require('./util/bytes');
var WritableUtils = require('./writable_utils');

function DataInputStream(io) {
  this.in = io;
  if (typeof io.read !== 'function') {
    this.in = new Readable();
    this.in.wrap(io);
  }
  this.bytearr = new Buffer(80);
}

DataInputStream.prototype.read = function (b, callback) {
  return this.in.read(b, 0, b.length);
};

DataInputStream.prototype.readBytes = function (size, callback) {
  var buf = this.in.read(size);
  debug('readBytes: %d size, Got %s, socket total read bytes: %d', size, buf ? 'Buffer' : null, this.in.bytesRead);
  if (buf === null) {
    return this.in.once('readable', this.readBytes.bind(this, size, callback));
  }
  callback(null, buf);
};

DataInputStream.prototype.readFields = function (fields, callback, startIndex, data) {
  var self = this;
  var lastError = null;
  data = data || {};
  var next = function (index) {
    if (index === fields.length) {
      return callback(lastError, data);
    }
    var field = fields[index];
    var nextIndex = index + 1;

    var value = self[field.method]();
    debug('readFields: %s index %d, name: %s, got %s, data %j, socket total read bytes: %d', 
      field.method, index, field.name, value, data, self.in.bytesRead);
    if (value === null) {
      // TODO: listeners too much
      return self.in.once('readable', self.readFields.bind(self, fields, callback, index, data));
    }
    data[field.name] = value;
    next(nextIndex);
  };
  startIndex = startIndex || 0;
  next(startIndex);
};

/**
 * See the general contract of the <code>readFully</code>
 * method of <code>DataInput</code>.
 * <p>
 * Bytes
 * for this operation are read from the contained
 * input stream.
 *
 * @param      len   the number of bytes to read.
 */
DataInputStream.prototype.readFully = function (len, callback) {
  var buf = this.in.read(len);
  if (buf === null) {
    return this.in.once('readable', this.readFully.bind(this, len, callback));
  }
  callback(null, buf);
};

/**
 * See the general contract of the <code>readBoolean</code>
 * method of <code>DataInput</code>.
 * <p>
 * Bytes for this operation are read from the contained
 * input stream.
 *
 * @return     the <code>boolean</code> value read.
 */
DataInputStream.prototype.readBoolean = function () {
  var buf = this.in.read(1);
  return buf ? buf[0] !== 0 : null;
};

/**
 * See the general contract of the <code>readByte</code>
 * method of <code>DataInput</code>.
 * <p>
 * Bytes
 * for this operation are read from the contained
 * input stream.
 *
 * @return     the next byte of this input stream as a signed 8-bit
 *             <code>byte</code>.
 */
DataInputStream.prototype.readByte = function () {
  var buf = this.in.read(1);
  return buf ? buf.readInt8(0) : null;
};

/**
 * See the general contract of the <code>readInt</code>
 * method of <code>DataInput</code>.
 * <p>
 * Bytes
 * for this operation are read from the contained
 * input stream.
 *
 * @return     the next four bytes of this input stream, interpreted as an
 *             <code>int</code>.
 */
DataInputStream.prototype.readInt = function () {
  var buf = this.in.read(4);
  return buf ? buf.readInt32BE(0) : null;
};

/**
 * See the general contract of the <code>readLong</code>
 * method of <code>DataInput</code>.
 * <p>
 * Bytes
 * for this operation are read from the contained
 * input stream.
 *
 * @return     the next eight bytes of this input stream, interpreted as a
 *             <code>long</code>.
 */
DataInputStream.prototype.readLong = function () {
  var buf = this.in.read(8);
  if (buf === null) {
    return buf;
  }
  return WritableUtils.toLong(buf);
};


module.exports = DataInputStream;
