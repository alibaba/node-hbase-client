/*!
 * node-hbase-client - lib/client.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Bytes = require('./bytes');
var DataOutputBuffer = require('./data_output_buffer');

function Client(options) {
  if (!(this instanceof Client)) {
    return new Client(options);
  }
  this.in = null;
  this.out = null;
  this.socket = null;
}

Client.create = function (options) {
  return new Client(options);
};

Client.prototype = {
  call: function (param, addr, protocol, ticket, rpcTimeout, callback) {
    rpcTimeout = rpcTimeout || 0;
    var call = new Call(param);
    call.on('done', function () {
      callback(call.error, call.value);
    });
    
    var connection = this.getConnection(addr, protocol, ticket, call);
    connection.sendParam(call); // send the parameter
  },

  /* Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  getConnection: function (addr, protocol, ticket, rpcTimeout, call) {
    // if (!running.get()) {
    //   // the client is stopped
    //   throw new IOException("The client is stopped");
    // }
    var connection;
    /* we could avoid this allocation for each RPC by having a
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    var remoteId = new ConnectionId(addr, protocol, ticket, rpcTimeout);
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(remoteId);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));

    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  },
};

var Call_Counter = 0;

/** A call waiting for a value. */
function Call(param) {
  this.id = Call_Counter++; // call id
  this.param = param; // parameter
  this.value = null; // value, null if error
  this.error = null; // exception, null if value
  this.done = false; // true when call is done
  this.startTime = Date.now();
}

util.inherits(Call, EventEmitter);

Call.prototype = {
  /** Indicate when the call is complete and the
   * value or error are available.  Notifies by default.  */
  callComplete: function () {
    this.done = true;
    this.emit('done', this);
  },

  /** Set the exception when there is an error.
   * Notify the caller the call is done.
   *
   * @param error exception thrown by the call; either local or remote
   */
  setException: function (error) {
    this.error = error;
    this.callComplete();
  },

  /** Set the return value when there is no error.
   * Notify the caller the call is done.
   *
   * @param value return value of the call.
   */
  setValue: function (value) {
    this.value = value;
    this.callComplete();
  },

  getStartTime: function () {
    return this.startTime;
  },
};



module.exports = Client;
