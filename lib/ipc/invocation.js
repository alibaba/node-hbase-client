/*!
 * node-hbase-client - lib/ipc/invocation.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');
var VersionedWritable = require('../io/version_writable');
var HbaseObjectWritable = require('../io/hbase_object_writable');

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
  this.clientVersion = 29; // HRegionInterface: public static final long VERSION = 29L;
  this.clientMethodsHash = 0;
  // if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
  //   //VersionedProtocol is exempted from version check.
  //   clientVersion = 0;
  //   clientMethodsHash = 0;
  // } else {
  //   var versionField = method.getDeclaringClass().getField("VERSION");
  //   versionField.setAccessible(true);
  //   this.clientVersion = versionField.getLong(method.getDeclaringClass());
  //   this.clientMethodsHash = ProtocolSignature.getFingerprint(method.getDeclaringClass().getMethods());
  // }
}

util.inherits(Invocation, VersionedWritable);

Invocation.prototype = {

  /** @return The name of the method invoked. */
  getMethodName: function () {
    return this.methodName;
  },

  /** @return The parameter classes. */
  getParameterClasses: function () {
    return this.parameterClasses;
  },

  /** @return The parameter instances. */
  getParameters: function () {
    return this.parameters;
  },

  getProtocolVersion: function () {
    return this.clientVersion;
  },

  getClientMethodsHash: function () {
    return this.clientMethodsHash;
  },

  /**
   * Returns the rpc version used by the client.
   * @return rpcVersion
   */
  getRpcVersion: function () {
    return RPC_VERSION;
  },

  // readFields: function (io) {
  //   Invocation.super_.prototype.readFields.call(this, io);
  //   this.methodName = io.readUTF();
  //   this.clientVersion = io.readLong();
  //   this.clientMethodsHash = io.readInt();
  //   // try {
  //   //   super.readFields(in);
  //   //   methodName = in.readUTF();
  //   //   clientVersion = in.readLong();
  //   //   clientMethodsHash = in.readInt();
  //   // } catch (VersionMismatchException e) {
  //   //   // VersionMismatchException doesn't provide an API to access
  //   //   // expectedVersion and foundVersion.  This is really sad.
  //   //   if (e.toString().endsWith("found v0")) {
  //   //     // Try to be a bit backwards compatible.  In previous versions of
  //   //     // HBase (before HBASE-3939 in 0.92) Invocation wasn't a
  //   //     // VersionedWritable and thus the first thing on the wire was always
  //   //     // the 2-byte length of the method name.  Because no method name is
  //   //     // longer than 255 characters, and all method names are in ASCII,
  //   //     // The following code is equivalent to `in.readUTF()', which we can't
  //   //     // call again here, because `super.readFields(in)' already consumed
  //   //     // the first byte of input, which can't be "unread" back into `in'.
  //   //     final short len = (short) (in.readByte() & 0xFF); // Unsigned byte.
  //   //     final byte[] buf = new byte[len];
  //   //     in.readFully(buf, 0, len);
  //   //     methodName = new String(buf);
  //   //   }
  //   // }
  //   this.parameters = new Object[in.readInt()];
  //   this.parameterClasses = new Class[parameters.length];
  //   var objectWritable = new HbaseObjectWritable();
  //   for (int i = 0; i < parameters.length; i++) {
  //     parameters[i] = HbaseObjectWritable.readObject(in, objectWritable, this.conf);
  //     parameterClasses[i] = objectWritable.getDeclaredClass();
  //   }
  // },

  write: function (out) {
    Invocation.super_.prototype.write.call(this, out);
    out.writeUTF(this.methodName);
    out.writeLong(this.clientVersion);
    out.writeInt(this.clientMethodsHash);
    out.writeInt(this.parameterClasses.length);
    for (var i = 0; i < this.parameterClasses.length; i++) {
      HbaseObjectWritable.writeObject(out, this.parameters[i], this.parameterClasses[i], this.conf);
    }
  },

  toString: function () {
    var buffer = this.methodName;
    buffer += "(";
    for (var i = 0; i < this.parameters.length; i++) {
      if (i !== 0) {
        buffer += ', ';
      }
      buffer += this.parameters[i];
    }
    buffer += ")";
    buffer += ", rpc version=" + RPC_VERSION;
    buffer += ", client version=" + this.clientVersion;
    buffer += ", methodsFingerPrint=" + this.clientMethodsHash;
    return buffer;
  },

  getVersion: function () {
    return RPC_VERSION;
  },
};


module.exports = Invocation;
