'use strict';

var util = require('util');

var Bytes = require('./util/bytes');
var errors = require('./errors');
var HConstants = require('./hconstants');
var OperationWithAttributes = require('./operation_with_attributes');

function CheckAndPut(row, family, qualifier, value, put) {
  OperationWithAttributes.call(this);
 
  if (row && !Buffer.isBuffer(row)) {
    row = Bytes.toBytes(row);
  }

  if (row === null || row.length > HConstants.MAX_ROW_LENGTH) {
    throw new errors.IllegalArgumentException("Row key is invalid");
  }

  if (family && !Buffer.isBuffer(family)) {
    family = Bytes.toBytes(family);
  }

  if (family === null) {
    throw new errors.IllegalArgumentException("Family is invalid");
  }

  if (qualifier && !Buffer.isBuffer(qualifier)) {
    qualifier = Bytes.toBytes(qualifier);
  }

  if (qualifier === null) {
    throw new errors.IllegalArgumentException("Qualifier is invalid");
  }

  if (value && !Buffer.isBuffer(value)) {
    value = Bytes.toBytes(value);
  }

  if (!value) {
    value = new Buffer([]);
  }

  this.value = value;
  this.row = row;
  this.family = family;
  this.qualifier = qualifier;
  this.put = put;
}

util.inherits(CheckAndPut, OperationWithAttributes);

CheckAndPut.prototype.getRow = function () {
  return this.row;
};

CheckAndPut.prototype.getFamily = function() {
  return this.family;
};

CheckAndPut.prototype.getQualifier = function() {
  return this.qualifier;
};

CheckAndPut.prototype.getValue = function() {
  return this.value;
};

CheckAndPut.prototype.getPut = function() {
  return this.put;
};

module.exports = CheckAndPut;
