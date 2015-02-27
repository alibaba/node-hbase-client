/**!
 * node-hbase-client - lib/filters/regex_string_comparator.js
 *
 *
 *
 * Authors:
 *   Takayuki Hasegawa <takayuki.hasegawa@gree.net>
 */

"use strict";

/**
 * Module dependencies.
 */
var Bytes = require('../util/bytes');

/**
 * This comparator is for use with {@link CompareFilter} implementations, such
 * as {@link RowFilter}, {@link QualifierFilter}, and {@link ValueFilter}, for
 * filtering based on the value of a given column. Use it to test if a given
 * regular expression matches a cell value in the column.
 * <p>
 * Only EQUAL or NOT_EQUAL comparisons are valid with this comparator.
 * <p>
 * For example:
 * <p>
 * <pre>
 * ValueFilter vf = new ValueFilter(CompareOp.EQUAL,
 *     new RegexStringComparator(
 *       // v4 IP address
 *       "(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3,3}" +
 *         "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(\\/[0-9]+)?" +
 *         "|" +
 *       // v6 IP address
 *       "((([\\dA-Fa-f]{1,4}:){7}[\\dA-Fa-f]{1,4})(:([\\d]{1,3}.)" +
 *         "{3}[\\d]{1,3})?)(\\/[0-9]+)?"));
 * </pre>
 */
function RegexStringComparator(expr) {
  if (!Buffer.isBuffer(expr)) {
    expr = new Buffer(expr);
  }
  this.pattern = expr;
  this.charset = "UTF-8";
}

RegexStringComparator.classname = 'org.apache.hadoop.hbase.filter.RegexStringComparator';

RegexStringComparator.prototype.toString = function () {
  return 'RegexStringComparator(pattern: ' + this.pattern + ', charset: ' + this.charset + ')';
};

RegexStringComparator.prototype.setCharset = function (charset) {
  this.charset = charset;
};

RegexStringComparator.prototype.write = function (out) {
  out.writeUTF(this.pattern);
  out.writeUTF(this.charset);
};

module.exports = RegexStringComparator;
