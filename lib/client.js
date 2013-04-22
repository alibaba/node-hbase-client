/*!
 * node-hbase-client - lib/client.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Bytes = require('./bytes');
var DataOutputBuffer = require('./data_output_buffer');

function Client (options) {
  this.counter = 0;
  this.in = null;
  this.out = null;
  this.socket = null;
}

Client.create = function (options) {
  return new Client(options);
};

Client.prototype = {
  call: function (param) {
    var callId = this.counter++;
    var d = new DataOutputBuffer();
    d.writeInt(0xdeadbeef); // placeholder for data length
    d.writeInt(callId);
    param.write(d);
    var data = d.getData();
    var dataLength = data.length;

    // fill in the placeholder
    Bytes.putInt(data, 0, dataLength - 4);
    this.socket.write(data);
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


module.exports = Client;
