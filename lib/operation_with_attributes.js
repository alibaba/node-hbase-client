/*!
 * node-hbase-client - lib/operation_with_attributes.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var eventproxy = require('eventproxy');
var Bytes = require('./util/bytes');
var WritableUtils = require('./writable_utils');

function OperationWithAttributes() {
  this.attributes = {};
}

/**
 * Set attribute.
 * 
 * @param {String} name
 * @param {Bytes} value
 */
OperationWithAttributes.prototype.setAttribute = function (name, value) {
  if (!this.attributes && (value === null || value === undefined)) {
    return;
  }

  if (!this.attributes) {
    this.attributes = {};
  }

  if (value === null || value === undefined) {
    delete this.attributes[name];
    if (Object.keys(this.attributes).length === 0) {
      this.attributes = null;
    }
  } else {
    this.attributes[name] = value;
  }
};

OperationWithAttributes.prototype.getAttribute = function (name) {
  if (!this.attributes) {
    return null;
  }
  return this.attributes[name];
};

OperationWithAttributes.prototype.getAttributesMap = function () {
  return this.attributes || {};
};

OperationWithAttributes.prototype.writeAttributes = function (out) {
  if (!this.attributes) {
    out.writeInt(0);
  } else {
    out.writeInt(Object.keys(this.attributes).length);
    for (var name in this.attributes) {
      WritableUtils.writeString(out, name);
      Bytes.writeByteArray(out, this.attributes[name]);
    }
  }
};

OperationWithAttributes.prototype.readAttributes = function (io) {
  var numAttributes = io.readInt();
  if (numAttributes > 0) {
    this.attributes = {};
    for (var i = 0; i < numAttributes; i++) {
      var name = io.readString();
      var value = io.readByteArray();
      this.attributes.put(name, value);
    }
  }
};


module.exports = OperationWithAttributes;
