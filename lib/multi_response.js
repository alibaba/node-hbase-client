/*!
 * node-hbase-client - lib/multi_response.js
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
var Pair = require('./pair');

function MultiResponse() {
  if (!(this instanceof MultiResponse)) {
    return new MultiResponse();
  }
  // map of regionName to list of (Results paired to the original index for that
  // Result)
  // Map<byte[], List<Pair<Integer, Object>>>
  this.results = {};
}

/**
 * @return Number of pairs in this container
 */
MultiResponse.prototype.size = function () {
  var size = 0;
  for (var key in results) {
    var lst = results[key];
    size += lst.length;
  }
  return size;
};

/**
 * @param  {DataInput} io [description]
 * @return {[type]}    [description]
 */
MultiResponse.prototype.readFields = function (io) {
  this.results = {};
  var mapSize = io.readInt();
  for (var i = 0; i < mapSize; i++) {
    var key = Bytes.toString(io.readByteArray());
    var listSize = io.readInt();
    //List<Pair<Integer, Object>> lst = new ArrayList<Pair<Integer, Object>>(listSize);
    var lst = [];
    for (var j = 0; j < listSize; j++) {
      var idx = io.readInt();
      if (idx == -1) {
        lst.add(null);
      } else {
        var isException = io.readBoolean();
        
        var o = null;
        if (isException) {
          var klass = io.readString();
          var desc = io.readString();
          //try {
            // the type-unsafe insertion, but since we control what klass is..
            // Class<? extends Throwable> c = (Class<? extends Throwable>) Class.forName(klass);
            // Constructor<? extends Throwable> cn = c.getDeclaredConstructor(String.class);
            // o = cn.newInstance(desc);
            o = new Error(desc);
            o.name = klass;
          //} catch (ex) {
          //} catch (ClassNotFoundException ignored) {
          //} catch (NoSuchMethodException ignored) {
          //} catch (InvocationTargetException ignored) {
          //} catch (InstantiationException ignored) {
          //} catch (IllegalAccessException ignored) {
          //}
        } else {
          o = HbaseObjectWritable.readObject(io, null);
        }
        lst.push(new Pair(idx, o));
      }
    }
    this.results[key] = lst;
  }
}

HbaseObjectWritable.addToClass('MultiResponse.class', MultiResponse);
module.exports = MultiResponse;