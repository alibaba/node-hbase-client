/**!
 * node-hbase-client - lib/filters/columnprefix.js
 *
 *
 *
 * Authors:
 *   Martin Cizek <martin.cizek@socialbakers.com> (http://github.com/wision)
 */

"use strict";

/**
 * Module dependencies.
 */

/**

 */
function ColumnPrefixFilter(prefix) {
  prefix = prefix === undefined ? '' : prefix;

  if (!Buffer.isBuffer(prefix)) {
    prefix = new Buffer(prefix);
  }

  this.prefix = prefix;
}

ColumnPrefixFilter.classname = 'org.apache.hadoop.hbase.filter.ColumnPrefixFilter';

ColumnPrefixFilter.prototype.write = function (out) {
  out.writeByte(this.prefix.length);
  out.writeBytes(this.prefix);
};

ColumnPrefixFilter.prototype.getClass = function () {
  return {getName: function() {return ColumnPrefixFilter.classname;}};
};

ColumnPrefixFilter.prototype.toString = function () {
  return 'ColumnPrefixFilter(prefix: ' + this.prefix + ')';
};

module.exports = ColumnPrefixFilter;
