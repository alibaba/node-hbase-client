/*jslint bitwise: true */
/*!
 * node-hbase-client - lib/ipc/resposer_flag.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

/**
 * Utility for managing the flag byte passed in response to a
 * {@link HBaseServer.Call}
 */
var ERROR_BIT = 0x1;
var LENGTH_BIT = 0x2;

exports.isError = function (flag) {
  return (flag & ERROR_BIT) !== 0;
};

exports.isLength = function (flag) {
  return (flag & LENGTH_BIT) !== 0;
};
