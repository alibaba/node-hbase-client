/*!
 * node-hbase-client - test/support/utils.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

require('buffer').INSPECT_MAX_BYTES = 1000;

var should = require('should');
var fs = require('fs');
var path = require('path');
var DataInputStream = require('../../lib/data_input_stream');
var DataInputBuffer = require('../../lib/data_input_buffer');
var DataOutputBuffer = require('../../lib/data_output_buffer');

var fixtures = path.join(path.dirname(__dirname), 'fixtures');
exports.fixtures = fixtures;

exports.checkBytes = function (bytes, javaBytes) {
  if (javaBytes.length !== bytes.length) {
    console.log('\njs  :', bytes, '\njava:', javaBytes);
  }
  bytes.length.should.equal(javaBytes.length);
  // console.log(v, bytes, javaBytes)
  // bytes.should.eql(javaBytes);
  for (var i = 0; i < bytes.length; i++) {
    if (bytes[i] !== javaBytes[i]) {
      console.log('\njs  :', bytes, '\njava:', javaBytes);
    }
    bytes[i].should.equal(javaBytes[i]);
  }
};

exports.createTestBytes = function (pathname) {
  return function (method, v, bytes) {
    var javaBytes = fs.readFileSync(path.join(fixtures, pathname, method + '_' + v + '.java.bytes'));

    // console.log('%s(%s): \njs:  ', method, v, bytes, '\njava:', javaBytes);
    if (javaBytes.length !== bytes.length) {
      console.log('%s(%s): \njs  :', method, v, bytes, '\njava:', javaBytes);
    }
    bytes.length.should.equal(javaBytes.length, method + ' ' + v);
    // console.log(v, bytes, javaBytes)
    // bytes.should.eql(javaBytes);
    for (var i = 0; i < bytes.length; i++) {
      if (bytes[i] !== javaBytes[i]) {
        console.log('%s(%s): \njs  :', method, v, bytes, '\njava:', javaBytes);
      }
      bytes[i].should.equal(javaBytes[i]);
    }
  };
};

exports.createDataInputBuffer = function (filename) {
  var filepath = path.join(fixtures, filename + '.java.bytes');
  return new DataInputBuffer(fs.readFileSync(filepath));
};

exports.createTestStream = function (dir, filename) {
  var filepath = path.join(fixtures, dir, filename + '.java.bytes');
  return new DataInputStream(fs.createReadStream(filepath));
};

exports.createNotServingRegionExceptionBuffer = function (id) {
  // id(4 bytes): readInt, flag(1 byte): readByte, size(4 bytes): readInt
  // state(4 bytes): readInt
  // name: readString, message: readString
  var buf = new DataOutputBuffer();
  buf.writeInt(id);
  buf.writeByte(0x3);
  var size = 4 + 4 + 4 + 9;
  var name = new Buffer('org.apache.hadoop.hbase.NotServingRegionException');
  var message = fs.readFileSync(path.join(fixtures, 'error', 'NotServingRegionException.bytes'));
  size += name.length + message.length;
  buf.writeInt(size);
  buf.writeInt(1); // state
  buf.writeString(name);
  buf.writeString(message);
  buf = buf.getData();
  var data = new Buffer(buf.length * 10);
  for (var i = 0; i < 10; i++) {
    buf.copy(data, i * buf.length);
  }
  return new DataInputBuffer(data);
};

exports.mockSocket = function () {
  return  {
    bytes: null,
    write: function (bytes, offset, length) {
      offset = offset || 0;
      length = length || bytes.length;
      if (!this.bytes) {
        this.bytes = bytes.slice(offset, length);
      } else {
        this.bytes = Buffer.concat([this.bytes, bytes.slice(offset, length)]);
      }
    }
  };
};
