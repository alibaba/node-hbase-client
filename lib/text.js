/*!
 * node-hbase-client - lib/text.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var WritableUtils = require('./writable_utils');

/** Read a UTF8 encoded string from in
 */
exports.readString = function (io, callback) {
  var length = WritableUtils.readVInt(io);
  io.readFully(0, length, function (err, buf) {
    if (err) {
      return callback(err);
    }
    callback(null, buf.toString('utf8'));
  });
};

/** Write a UTF8 encoded string to out
 */
exports.writeString = function (out, s) {
  var bytes = new Buffer(s, 'utf8');
  var length = bytes.length();
  WritableUtils.writeVInt(out, length);
  out.write(bytes);
  return length;
};


