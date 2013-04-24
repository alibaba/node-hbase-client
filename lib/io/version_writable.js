/*!
 * node-hbase-client - lib/io/version_writable.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var VersionMismatchException = require('../errors').VersionMismatchException;

function VersionedWritable() {

}

VersionedWritable.prototype = {
  /** Return the version number of the current implementation. */
  getVersion: function () {
    throw new Error('Not Implemented');
  },
  write: function (out) {

  },
  readFields: function (io) {
    var version = io.readByte(); // read version
    if (version !== this.getVersion()) {
      throw new VersionMismatchException(this.getVersion(), version);
    }
  },
};


module.exports = VersionedWritable;
