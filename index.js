/*!
 * node-hbase-client - index.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

exports.TimeRange = require('./lib/time_range');
exports.Get = require('./lib/get');
exports.Put = require('./lib/put');
exports.Scan = require('./lib/scan');
exports.Result = require('./lib/result');
exports.Client = require('./lib/client');
exports.Delete = require('./lib/delete');
exports.filters = require('./lib/filters');

exports.create = exports.Client.create;
