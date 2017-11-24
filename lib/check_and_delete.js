'use strict';

var Bytes = require('./util/bytes');
var errors = require('./errors');
var OperationWithAttributes = require('./operation_with_attributes');
var util = require('util');

function CheckAndDelete(row, family, qualifier, value, deleter) {
  OperationWithAttributes.call(this);
 
  if (row && !Buffer.isBuffer(row)) {
    row = Bytes.toBytes(row);
  } else if (!row) {
    throw new errors.IllegalArgumentException("Row key is invalid");
  }

  if (family && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  } else if (!family) {
    throw new errors.IllegalArgumentException("Family is invalid");
  }

  if (qualifier && !Buffer.isBuffer(qualifier)) {
    qualifier = Bytes.toBytes(qualifier);
  } else if (!qualifier) {
    throw new errors.IllegalArgumentException("Qualifier is invalid");
  }

  if (value && !Buffer.isBuffer(value)) {
    value = Bytes.toBytes(value);
  } else if (!value) {
    value = Bytes.toBytes('');
  }

  this.value = value;
  this.row = row;
  this.family = family;
  this.qualifier = qualifier;
  this.delete = deleter;
}

util.inherits(CheckAndDelete, OperationWithAttributes);

CheckAndDelete.prototype.getRow = function () {
  return this.row;
};

CheckAndDelete.prototype.getFamily = function() {
  return this.family;
};

CheckAndDelete.prototype.getQualifier = function() {
  return this.qualifier;
};

CheckAndDelete.prototype.getValue = function() {
  return this.value;
};

CheckAndDelete.prototype.getDelete = function() {
  return this.delete;
};

module.exports = CheckAndDelete;
