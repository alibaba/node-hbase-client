/*!
 * node-hbase-client - lib/scanner.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');

function Scanner(server, id) {
  EventEmitter.call(this);
  
  this.server = server;
  this.id = id;
}

util.inherits(Scanner, EventEmitter);

Scanner.prototype.next = function (numberOfRows, callback) {
  this.server.nextResult(this.id, numberOfRows, callback);
};

Scanner.prototype.close = function (callback) {
  this.server.closeScanner(this.id, callback);
};


module.exports = Scanner;
