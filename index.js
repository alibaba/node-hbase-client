/*!
 * node-hbase-client - index.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var libdir = process.env.NODE_HBASE_CLENT_COV ? './lib-cov' : './lib';

exports.TimeRange = require(libdir + '/time_range');
exports.Get = require(libdir + '/get');
exports.OutStream = require(libdir + '/out_stream');
exports.InStream = require(libdir + '/in_stream');
