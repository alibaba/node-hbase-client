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

/**
 * Reads some number of bytes from the contained input stream and 
 * stores them into the buffer array <code>b</code>. The number of 
 * bytes actually read is returned as an integer. This method blocks 
 * until input data is available, end of file is detected, or an 
 * exception is thrown. 
 * 
 * <p>If <code>b</code> is null, a <code>NullPointerException</code> is 
 * thrown. If the length of <code>b</code> is zero, then no bytes are 
 * read and <code>0</code> is returned; otherwise, there is an attempt 
 * to read at least one byte. If no byte is available because the 
 * stream is at end of file, the value <code>-1</code> is returned;
 * otherwise, at least one byte is read and stored into <code>b</code>. 
 * 
 * <p>The first byte read is stored into element <code>b[0]</code>, the 
 * next one into <code>b[1]</code>, and so on. The number of bytes read 
 * is, at most, equal to the length of <code>b</code>. Let <code>k</code> 
 * be the number of bytes actually read; these bytes will be stored in 
 * elements <code>b[0]</code> through <code>b[k-1]</code>, leaving 
 * elements <code>b[k]</code> through <code>b[b.length-1]</code> 
 * unaffected. 
 * 
 * <p>The <code>read(b)</code> method has the same effect as: 
 * <blockquote><pre>
 * read(b, 0, b.length) 
 * </pre></blockquote>
 *
 * @param      b   the buffer into which the data is read.
 * @return     the total number of bytes read into the buffer, or
 *             <code>-1</code> if there is no more data because the end
 *             of the stream has been reached.
 * @exception  IOException if the first byte cannot be read for any reason
 * other than end of file, the stream has been closed and the underlying
 * input stream does not support reading after close, or another I/O
 * error occurs.
 * @see        java.io.FilterInputStream#in
 * @see        java.io.InputStream#read(byte[], int, int)
 */
DataInputStream.prototype.read = function (b, callback) {
  return this.in.read(b, 0, b.length);
};

DataInputStream.prototype.readBytes = function (size, callback) {
  var buf = this.in.read(size);
  debug('readBytes: %d size, Got %s', size, buf ? 'Buffer' : null);
  if (buf === null) {
    return this.in.once('readable', this.readBytes.bind(this, size, callback));
  }
  callback(null, buf);
};

DataInputStream.prototype.readFields = function (fields, callback, startIndex) {
  var self = this;
  var lastError = null;
  var data = {};
  var next = function (index) {
    if (index === fields.length) {
      return callback(lastError, data);
    }
    var field = fields[index];
    var nextIndex = index + 1;

    var value = self[field.method]();
    debug('readFields: %s index %d, name: %s, got %s', field.method, index, field.name, value);
    if (value === null) {
      // TODO: listeners too much
      return self.in.once('readable', self.readFields.bind(self, fields, callback, index));
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
 * @param      b     the buffer into which the data is read.
 * @param      off   the start offset of the data.
 * @param      len   the number of bytes to read.
 * @exception  EOFException  if this input stream reaches the end before
 *               reading all the bytes.
 * @exception  IOException   the stream has been closed and the contained
 *       input stream does not support reading after close, or
 *       another I/O error occurs.
 * @see        java.io.FilterInputStream#in
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
 * @exception  EOFException  if this input stream has reached the end.
 * @exception  IOException   the stream has been closed and the contained
 *       input stream does not support reading after close, or
 *       another I/O error occurs.
 * @see        java.io.FilterInputStream#in
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
 * @exception  EOFException  if this input stream has reached the end.
 * @exception  IOException   the stream has been closed and the contained
 *       input stream does not support reading after close, or
 *       another I/O error occurs.
 * @see        java.io.FilterInputStream#in
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
 * @exception  EOFException  if this input stream reaches the end before
 *               reading four bytes.
 * @exception  IOException   the stream has been closed and the contained
 *       input stream does not support reading after close, or
 *       another I/O error occurs.
 * @see        java.io.FilterInputStream#in
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
 * @exception  EOFException  if this input stream reaches the end before
 *               reading eight bytes.
 * @exception  IOException   the stream has been closed and the contained
 *       input stream does not support reading after close, or
 *       another I/O error occurs.
 * @see        java.io.FilterInputStream#in
 */
DataInputStream.prototype.readLong = function () {
  var buf = this.in.read(8);
  if (buf === null) {
    return buf;
  }
  return WritableUtils.toLong(buf);
};


module.exports = DataInputStream;
