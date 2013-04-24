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

ConnectionId.prototype = {

  getAddress: function () {
    return this.address;
  },

  getProtocol: function () {
    return this.protocol;
  },

  getTicket: function () {
    return this.ticket;
  },

  // @Override
  // public boolean equals(Object obj) {
  //   if (obj instanceof ConnectionId) {
  //     ConnectionId id = (ConnectionId) obj;
  //     return address.equals(id.address) && protocol == id.protocol
  //         && ((ticket != null && ticket.equals(id.ticket)) || (ticket == id.ticket)) && rpcTimeout == id.rpcTimeout;
  //   }
  //   return false;
  // }

  // @Override
  // // simply use the default Object#hashcode() ?
  // public int hashCode() {
  //   return (address.hashCode() + PRIME
  //       * (PRIME * System.identityHashCode(protocol) ^ (ticket == null ? 0 : ticket.hashCode())))
  //       ^ rpcTimeout;
  // }
};


module.exports = ConnectionId;
