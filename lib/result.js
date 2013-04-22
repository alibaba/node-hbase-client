/*!
 * node-hbase-client - lib/result.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var HbaseObjectWritable = require('./io/hbase_object_writable');
var RESULT_VERSION = 1;

function Result() {
  this.kvs = null;
  this.familyMap = null;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  this.row = null;
  this.bytes = null;
}

Result.prototype = {

};


HbaseObjectWritable.addToClass('Result.class', Result);
module.exports = Result;
