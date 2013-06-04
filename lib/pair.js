/*!
 * node-hbase-client - lib/pair.js
 * Copyright(c) 2013 tangyao<tangyao@alibaba-inc.com>
 * MIT Licensed
 */

'use strict';

function Pair(a, b) {
  this.first = a;
  this.second = b;
}

/**
 * Return the first element stored in the pair.
 * @return T1
 */
Pair.prototype.getFirst = function () {
  return this.first;
};

/**
 * Return the second element stored in the pair.
 * @return T2
 */
Pair.prototype.getSecond = function () {
  return this.second;
};

Pair.prototype.toString = function () {
  return "{" + this.getFirst() + "," + this.getSecond() + "}";
};


module.exports = Pair;
