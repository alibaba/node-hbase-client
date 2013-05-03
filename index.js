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

// utils
exports.Bytes = require(libdir + '/util/bytes');
exports.WritableUtils = require(libdir + '/writable_utils');
exports.HConstants = require(libdir + '/hconstants');

// base object
exports.TimeRange = require(libdir + '/time_range');
exports.Get = require(libdir + '/get');
exports.Put = require(libdir + '/put');
exports.Scan = require(libdir + '/scan');
exports.Result = require(libdir + '/result');
exports.HRegionInfo = require(libdir + '/hregion_info');

// io
exports.HbaseObjectWritable = require(libdir + '/io/hbase_object_writable');
exports.DataOutputBuffer = require(libdir + '/data_output_buffer');
exports.DataInputStream = require(libdir + '/data_input_stream');
exports.DataInputBuffer = require(libdir + '/data_input_buffer');

// ipc
exports.Invocation = require(libdir + '/ipc/invocation');
exports.ConnectionId = require(libdir + '/ipc/connection_id');

exports.Connection = require(libdir + '/connection');
exports.Client = require(libdir + '/client');

exports.create = exports.Client.create;
