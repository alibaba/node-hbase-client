/*!
 * node-hbase-client - lib/errors.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');

// Create a new Abstract Error constructor
var AbstractError = function (msg, constr) {
  // http://dustinsenos.com/articles/customErrorsInNode
  // 
  // If defined, pass the constr property to V8's
  // captureStackTrace to clean up the output
  Error.captureStackTrace(this, constr || this);

  // If defined, store a custom error message
  this.message = msg || 'Error';
};

// Extend our AbstractError from Error
util.inherits(AbstractError, Error);

// Give our Abstract error a name property. Helpful for logging the error later.
AbstractError.prototype.name = 'AbstractError';

var IOException = function (msg) {
  IOException.super_.call(this, msg, this.constructor);
};

util.inherits(IOException, AbstractError);
IOException.prototype.name = 'IOException';

module.exports = {
  AbstractError: AbstractError,
  IOException: IOException,
};
