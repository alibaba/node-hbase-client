/*!
 * node-hbase-client - lib/action.js
 * Copyright(c) 2013 tangyao<tangyao@alibaba-inc.com>
 * MIT Licensed
 */

'use strict';

var util = require('util');
var errors = require('./errors');
var KeyValue = require('./keyvalue');
var Bytes = require('./util/bytes');
var OperationWithAttributes = require('./operation_with_attributes');
var HConstants = require('./hconstants');
var HbaseObjectWritable = require('./io/hbase_object_writable');

/*
 * A Get, Put or Delete associated with it's region.  Used internally by
 * {@link HTable::batch} to associate the action with it's region and maintain
 * the index from the original request.
 * @param {Row} action
 * @param {int} originalIndex
 */
function Action(action, originalIndex) {
  this.action = action;
  this.originalIndex = originalIndex;
  this.result = null;
}

Action.prototype.write = function (out) {
  HbaseObjectWritable.writeObject(out, this.action);
  out.writeInt(this.originalIndex);
  // write null: null, 'Writable.class'
  HbaseObjectWritable.writeObject(out, this.result, this.result ? this.result.constructor : 'Writable.class');
};


module.exports = Action;
