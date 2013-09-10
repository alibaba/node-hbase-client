/*!
 * node-hbase-client - lib/ipc/invocation.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase:ipc:invocation');
var util = require('util');
var VersionedWritable = require('../io/version_writable');
var HbaseObjectWritable = require('../io/hbase_object_writable');
var HConstants = require('../hconstants');

var RPC_VERSION = 1;

/** A method invocation, including the method name and its parameters.*/
function Invocation(method, parameters) {
  Invocation.super_.call(this);
  // method: {name, parameterTypes, parameters}
  // name: 'get', parameterTypes: ['Buffer', 'Get.class'], parameters: [buf, get]
  // public Invocation(Method method, Object[] parameters) {
  this.methodName = method;
  this.parameterClasses = [];
  this.parameters = parameters;
  for (var i = 0; i < parameters.length; i++) {
    this.parameterClasses.push(parameters[i].constructor);
  }
  this.clientVersion = HConstants.CLIENT_VERSION; // HRegionInterface: public static final long VERSION = 29L;
  this.clientMethodsHash = 0;
  if (method === 'getProtocolVersion') {
    this.clientVersion = 0;
    this.clientMethodsHash = 0;
  }
}

util.inherits(Invocation, VersionedWritable);

Invocation.prototype.write = function (out) {
  Invocation.super_.prototype.write.call(this, out);
  out.writeUTF(this.methodName);
  out.writeLong(this.clientVersion);
  out.writeInt(this.clientMethodsHash);
  out.writeInt(this.parameters.length);
  debug('writable: method: %s, clientVersion: %s, clientMethodsHash: %s, parameters len: %d', 
    this.methodName, this.clientVersion, this.clientMethodsHash, this.parameters.length);
  for (var i = 0; i < this.parameters.length; i++) {
    HbaseObjectWritable.writeObject(out, this.parameters[i], this.parameterClasses[i]);
  }
};

Invocation.prototype.getVersion = function () {
  return RPC_VERSION;
};


module.exports = Invocation;
