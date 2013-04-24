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

/**
 * Returns a hash code for this string. The hash code for a
 * <code>String</code> object is computed as
 * <blockquote><pre>
 * s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
 * </pre></blockquote>
 * using <code>int</code> arithmetic, where <code>s[i]</code> is the
 * <i>i</i>th character of the string, <code>n</code> is the length of
 * the string, and <code>^</code> indicates exponentiation.
 * (The hash value of the empty string is zero.)
 *
 * @return  a hash code value for this object.
 */
// exports.hashCode = function (str) {
//   var h = str.hash || 0;
//   var len = str.length;
//   if (h === 0 && len > 0) {
//     var off = offset;
//     char val[] = value;

//         for (int i = 0; i < len; i++) {
//             h = 31*h + val[off++];
//         }
//         hash = h;
//     }
//     return h;
// }

