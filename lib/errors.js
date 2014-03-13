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


// Runtime
var RuntimeException = function (msg) {
  RuntimeException.super_.call(this, msg, this.constructor);
};
util.inherits(RuntimeException, AbstractError);
RuntimeException.prototype.name = 'RuntimeException';

var UnsupportedOperationException = function (msg) {
  UnsupportedOperationException.super_.call(this, msg, this.constructor);
};
util.inherits(UnsupportedOperationException, RuntimeException);
UnsupportedOperationException.prototype.name = 'UnsupportedOperationException';


// IO
var IOException = function (msg) {
  IOException.super_.call(this, msg, this.constructor);
};
util.inherits(IOException, AbstractError);
IOException.prototype.name = 'IOException';

/**
 * Thrown when something happens related to region handling.
 * Subclasses have to be more specific.
 */
var RegionException = function (msg) {
  RegionException.super_.call(this, msg, this.constructor);
};
util.inherits(RegionException, IOException);
RegionException.prototype.name = 'RegionException';

/** Thrown when a table can not be located */
var RegionOfflineException = function (msg) {
  RegionOfflineException.super_.call(this, msg, this.constructor);
};
util.inherits(RegionOfflineException, RegionException);
RegionOfflineException.prototype.name = 'RegionOfflineException';

/**
 * Thrown when no region server can be found for a region
 */
var NoServerForRegionException = function (msg) {
  NoServerForRegionException.super_.call(this, msg, this.constructor);
};
util.inherits(NoServerForRegionException, RegionException);
NoServerForRegionException.prototype.name = 'NoServerForRegionException';

var TableNotFoundException = function (msg) {
  TableNotFoundException.super_.call(this, msg, this.constructor);
};
util.inherits(TableNotFoundException, RegionException);
TableNotFoundException.prototype.name = 'TableNotFoundException';

var NoServerForRegionException = function (msg) {
  NoServerForRegionException.super_.call(this, msg, this.constructor);
};
util.inherits(NoServerForRegionException, RegionException);
NoServerForRegionException.prototype.name = 'NoServerForRegionException';

var RemoteException = function (className, msg) {
  RemoteException.super_.call(this, msg, this.constructor);
  this.name = className;
};
util.inherits(RemoteException, IOException);
RemoteException.prototype.name = 'RemoteException';

var ConnectionClosedException = function (msg) {
  ConnectionClosedException.super_.call(this, msg, this.constructor);
};
util.inherits(ConnectionClosedException, IOException);
ConnectionClosedException.prototype.name = 'ConnectionClosedException';

var ConnectionConnectTimeoutException = function (msg) {
  ConnectionConnectTimeoutException.super_.call(this, msg, this.constructor);
};
util.inherits(ConnectionConnectTimeoutException, IOException);
ConnectionConnectTimeoutException.prototype.name = 'ConnectionConnectTimeoutException';

var RemoteCallTimeoutException = function (msg) {
  RemoteCallTimeoutException.super_.call(this, msg, this.constructor);
};
util.inherits(RemoteCallTimeoutException, IOException);
RemoteCallTimeoutException.prototype.name = 'RemoteCallTimeoutException';

var VersionMismatchException = function (expectedVersion, foundVersion) {
  var msg = "A record version mismatch occured. Expecting v" + expectedVersion + ", found v" + foundVersion;
  VersionMismatchException.super_.call(this, msg, this.constructor);
};
util.inherits(VersionMismatchException, IOException);
VersionMismatchException.prototype.name = 'VersionMismatchException';


// Argument
var IllegalArgumentException = function (msg) {
  IllegalArgumentException.super_.call(this, msg, this.constructor);
};
util.inherits(IllegalArgumentException, AbstractError);
IllegalArgumentException.prototype.name = 'IllegalArgumentException';

var NegativeArraySizeException = function (msg) {
  NegativeArraySizeException.super_.call(this, msg, this.constructor);
};
util.inherits(NegativeArraySizeException, AbstractError);
NegativeArraySizeException.prototype.name = 'NegativeArraySizeException';



module.exports = {
  AbstractError: AbstractError,

  // IO
  IOException: IOException,
  RegionException: RegionException,
  TableNotFoundException: TableNotFoundException,
  RemoteException: RemoteException,
  VersionMismatchException: VersionMismatchException,
  RemoteCallTimeoutException: RemoteCallTimeoutException,
  ConnectionClosedException: ConnectionClosedException,
  ConnectionConnectTimeoutException: ConnectionConnectTimeoutException,

  RegionOfflineException: RegionOfflineException,

  // Runtime
  RuntimeException: RuntimeException,
  UnsupportedOperationException: UnsupportedOperationException,


  IllegalArgumentException: IllegalArgumentException,
  NegativeArraySizeException: NegativeArraySizeException,

};
