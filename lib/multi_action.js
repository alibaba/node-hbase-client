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
 * @param {regionInfo} regionName
 * @param {Action<R>} action
 */
MultiAction.prototype.add = function (regionInfo, action) {
  var key = regionInfo.regionNameStr;
  var rsActions = this.actions[key];
  if (!rsActions) {
    rsActions = this.actions[key] = {regionInfo: regionInfo, list: []};
  }
  rsActions.list.push(action);
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
  for (var key in this.actions) {
    var obj = this.actions[key];
    Bytes.writeByteArray(out, obj.regionInfo.getRegionName());
    var lst = obj.list;
    out.writeInt(lst.length);
    for (var i = 0; i < lst.length; i++) {
      var action = lst[i];
      HbaseObjectWritable.writeObject(out, action); // TODO:
    }
  }
};


module.exports = MultiAction;
