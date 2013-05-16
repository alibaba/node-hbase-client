/*!
 * node-hbase-client - lib/multi_action.js
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

function MultiAction() {
  // map of regions to lists of puts/gets/deletes for that region.
  // <byte[], List<Action<R>>>
  this.actions = {};
}

MultiAction.prototype.size = function () {
  var size = 0;
  for (var key in this.actions) {
    var list = this.actions[key];
    size += list.length;
  }
  return size;
};

/**
 * Add an Action to this container based on it's regionName. If the regionName
 * is wrong, the initial execution will fail, but will be automatically
 * retried after looking up the correct region.
 *
 * @param {byte[]} regionName
 * @param {Action<R>} action
 */
MultiAction.prototype.add = function (regionName, action) {
  /*if (regionName !== null && !Buffer.isBuffer(regionName)) {
    regionName = Bytes.toBytes(regionName);
  }*/
  var rsActions = this.actions[regionName];
  if (!rsActions) {
    rsActions = this.actions[regionName] = [];
  }
  rsActions.push(action);
};

/**
 * @return Set<byte[]>
 */
MultiAction.prototype.getRegions = function () {
  return Object.keys(this.actions);
};

/**
 * @param {DataOutput} out
 */
MultiAction.prototype.write = function (out) {
  out.writeInt(Object.keys(this.actions).length);
  for (var regionName in this.actions) {
    Bytes.writeByteArray(out, Bytes.toBytes(regionName));
    var lst = this.actions[regionName];
    out.writeInt(lst.length);
    for (var i = 0; i < lst.length; i++) {
      var action = lst[i];
      HbaseObjectWritable.writeObject(out, action); // TODO:
    }
  }
}

module.exports = MultiAction;