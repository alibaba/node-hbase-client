/*!
 * node-hbase-client - lib/ipc/connection_id.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var PRIME = 16777619;

/**
 * This class holds the address and the user ticket. The client connections
 * to servers are uniquely identified by <remoteAddress, ticket>
 */
function ConnectionId(address, protocol, ticket, rpcTimeout) {
  // {port, host}
  this.address = address;
  this.ticket = ticket;
  this.rpcTimeout = rpcTimeout;
  this.protocol = protocol;
  this.hash = address.host + address.port + PRIME + 
    (protocol ? protocol.toString() : '') + (rpcTimeout ? rpcTimeout.toString() : '');
}

ConnectionId.prototype.getAddress = function () {
  return this.address;
},

ConnectionId.prototype.getProtocol = function () {
  return this.protocol;
};

ConnectionId.prototype.getTicket = function () {
  return this.ticket;
};

ConnectionId.prototype.toString = function () {
  return 'Connection:(' + this.hash + ')';
};


module.exports = ConnectionId;
