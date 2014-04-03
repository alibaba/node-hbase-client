/*!
 * node-hbase-client - lib/connection.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase:connection');
var Long = require('long');
var Readable = require('readable-stream').Readable;
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var utility = require('utility');
var net = require('net');
var Text = require('./text');
var Bytes = require('./util/bytes');
var ResponseFlag = require('./ipc/response_flag');
var DataInputBuffer = require('./data_input_buffer');
var DataOutputBuffer = require('./data_output_buffer');
var DataInputStream = require('./data_input_stream');
var DataOutputStream = require('./data_output_stream');
var HbaseObjectWritable = require('./io/hbase_object_writable');
var Invocation = require('./ipc/invocation');
var HConstants = require('./hconstants');
var errors = require('./errors');
var RemoteException = errors.RemoteException;

var HEADER = new Buffer("hrpc", "utf8");
var CURRENT_VERSION = 3;
var PING_CALL_ID = new Buffer(4);
PING_CALL_ID.writeInt32BE(-1, 0);

/**
 * The IPC connection header sent by the client to the server
 * on connection establishment.
 *
 * Create a new {@link ConnectionHeader} with the given <code>protocol</code>
 * and {@link User}.
 * @param protocol protocol used for communication between the IPC client
 *                 and the server
 * @param user {@link User} of the client communicating with
 *            the server
 */
function ConnectionHeader(protocol, user) {
  this.protocol = protocol;
  this.user = user;
}

ConnectionHeader.prototype.write = function (out) {
  Text.writeString(out, this.protocol || '');
},

ConnectionHeader.prototype.getProtocol = function () {
  return this.protocol;
};

ConnectionHeader.prototype.getUser = function () {
  return null;
};

ConnectionHeader.prototype.toString = function () {
  return this.protocol;
};

var _Connection_id = 0;

/** Thread that reads responses and notifies callers.  Each connection owns a
 * socket connected to a remote address.  Calls are multiplexed through this
 * socket: responses may be delivered out of order. */
function Connection(options) {
  EventEmitter.call(this);
  this.id = _Connection_id++;
  this.header = null; // connection header
  this.socket = null; // connected socket
  this.in;
  this.out;

  this.tcpNoDelay = false; // nodelay not or
  this.tcpKeepAlive = true;

  // currently active calls
  this.calls = {};

  this.address = {host: options.host, port: options.port};
  this.hostnamePort = options.host + ':' + options.port;
  this.name = 'Connection(' + this.hostnamePort + ')#' + this.id;
  var protocol = options.protocol || HConstants.PROTOCOL;
  this.rpcTimeout = options.rpcTimeout || HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
  this.pingInterval = options.pingInterval || HConstants.DEFAULT_PING_INTERVAL;

  this.logger = options.logger || console;
  this.header = new ConnectionHeader(protocol, options.ticket);
  this._connected = false;
  this._socketError = null;
  this.setupIOstreams();
  this._callNums = 0;
}

util.inherits(Connection, EventEmitter);

Connection.prototype.setupConnection = function () {
  var ioFailures = 0;
  var timeoutFailures = 0;
  this.socket = net.connect(this.address);
  this.socketReadable = this.socket;
  if (typeof this.socketReadable.read !== 'function') {
    this.socketReadable = new Readable();
    this.socketReadable.wrap(this.socket);
    // ignore error event
    this.socketReadable.on('error', utility.noop);
  }
  this.socket.setNoDelay(this.tcpNoDelay);
  this.socket.setKeepAlive(this.tcpKeepAlive);
  // if (this.remoteId.rpcTimeout > 0) {
  //   this.pingInterval = this.remoteId.rpcTimeout;
  // }
  // this.socket.setTimeout(this.pingInterval);
  this.socket.on('timeout', this._handleTimeout.bind(this));
  this.socket.on('close', this._handleClose.bind(this));

  // when error, response all calls error
  this.socket.on('error', this._handleError.bind(this));

  // send ping
  this._pingTimer = setInterval(this.sendPing.bind(this), this.pingInterval);
};

Connection.prototype._cleanupCalls = function (err) {
  var count = 0;
  var calls = this.calls;
  // should reset calls before traverse it.
  this.calls = {};
  for (var id in calls) {
    var call = calls[id];
    call.setException(err);
    count++;
  }
  this.logger.warn('%s: cleanup %d calls, send "%s:%s" response.', this.name, count, err.name, err.message);
  // clean timer
  if (this._pingTimer) {
    clearInterval(this._pingTimer);
    this._pingTimer = null;
  }
};

Connection.prototype._handleTimeout = function () {
  this._close(new errors.ConnectionClosedException(this.name + ' socket close by "timeout" event.'));
};

Connection.prototype._handleClose = function () {
  debug('%s `close` event emit', this.name);
  this.closed = true;
  // tell user not use this connection first
  this.emit('close'); // let client close first.

  var err = this._closeError || this._socketError;
  if (!err) {
    err = new errors.ConnectionClosedException(this.name + ' closed with no error.');
    this.logger.warn(err.message);
  }
  this._cleanupCalls(err);
};

Connection.prototype._handleError = function (err) {
  debug('%s `error` event emit: %s', this.name, err);
  if (err.message.indexOf('ECONNREFUSED') >= 0) {
    err.name = 'ConnectionRefusedException';
  }
  if (err.message.indexOf('ECONNRESET') >= 0 || err.message.indexOf('This socket is closed') >= 0) {
    err.name = 'ConnectionClosedException';
  }
  this._socketError = err;
  if (!this._connected) {
    this.emit('connectError', err);
  }
  this._cleanupCalls(err);
};

/** Connect to the server and set up the I/O streams. It then sends
 * a header to the server and starts
 * the connection thread that waits for responses.
 * @throws java.io.IOException e
 */
Connection.prototype.setupIOstreams = function () {
  var self = this;
  debug('Connecting to %s', self.name);
  self.setupConnection();
  self.in = new DataInputStream(self.socketReadable);
  self.out = new DataOutputStream(self.socket);

  self.socket.on('connect', function () {
    this._connected = true;
    self.writeHeader();
    self._nextResponse();
    self.emit('connect');
    debug('Connected to %s', self.name);
  });
};

/* Write the header for each connection
 * Out is not synchronized because only the first thread does this.
 */
Connection.prototype.writeHeader = function () {
  this.out.write(HEADER);
  this.out.writeByte(CURRENT_VERSION);
  //When there are more fields we can have ConnectionHeader Writable.
  var buf = new DataOutputBuffer();
  this.header.write(buf);

  var bufLen = buf.getLength();
  this.out.writeInt(bufLen);
  this.out.write(buf.getData());
};

var _pingCount = 0;
Connection.prototype.sendPing = function () {
  debug('%s sendPing #%d', this.name, _pingCount++);
  this.out.write(PING_CALL_ID);
};

/* Initiates a call by sending the parameter to the remote server.
 * Note: this is not called from the Connection thread, but by other
 * threads.
 */
Connection.prototype.sendParam = function (call) {
  // For serializing the data to be written.

  //this.logger.info('%s sending #%s', this.name, call.id);

  var d = new DataOutputBuffer();
  d.writeInt(0); // placeholder for data length
  d.writeInt(call.id);
  call.param.write(d);
  var data = d.getData();
  var dataLength = d.getLength();

  // fill in the placeholder
  Bytes.putInt(data, 0, dataLength - 4);
  this.out.write(data, 0, dataLength);
};

Connection.prototype._close = function (err) {
  this.closed = true;
  this._closeError = err;
  // TODO: end or close?
  // this.socket.end();
  this.logger.warn('%s socket destroy().', this.name);
  this.socket.destroy();
};

Connection.prototype.close = Connection.prototype._close;

// ignore errors, no need to close the current connection
var IGNORE_EXCEPTIONS = {
  'org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException': true,
  'org.apache.hadoop.hbase.regionserver.WrongRegionException': true,
  'org.apache.hadoop.hbase.NotServingRegionException': true,
};

Connection.prototype._nextResponse = function () {
  var self = this;
  // See HBaseServer.Call.setResponse for where we write out the response.
  // It writes the call.id (int), a flag byte, then optionally the length
  // of the response (int) followed by data.
  self.in.readFields([
    {name: 'id', method: 'readInt'},
    {name: 'flag', method: 'readByte'},
    {name: 'size', method: 'readInt'},
  ], function (err, data) {

    if (!data.hasOwnProperty('id') || !data.hasOwnProperty('flag') || !data.hasOwnProperty('size')) {
      self.logger.warn('[ERROR] [%s] data format wrong: %j, keys: %j', Date(), data, Object.keys(data));
      var msg = 'data packet wrong, data: ' + JSON.stringify(data);
      debug('%s: %s', self.name, msg);
      err = new RemoteException('RemoteException', msg);
      self._close(err);
      return;
    }

    // Read the call id.
    var id = data && data.id;
    var size = data.size - 9; // remove header size, Int, Byte, Int, 9 bytes
    var flag = data.flag;
    var isError = ResponseFlag.isError(flag);
    debug('receiveResponse: got %s:call#%s response, flag: %s, isError: %s, size: %s',
      self.name, id, flag, isError, size);

    var call = self.calls[id];

    if (!call) {
      // call timeout event will cause connection remove the call
      debug('[WARNNING] [%s] %s:call#%s not exists, data: %j', Date(), id, data);
    } else {
      delete self.calls[id];
    }

    if (err) {
      call && call.setException(err);
      self._close();
      return;
    }

    if (typeof flag !== 'number' || !ResponseFlag.isLength(flag)) {
      debug('%s:call#%s missing data length packet, flag: %s', self.name, id, flag);
      err = new RemoteException('RemoteException', 'missing data length packet, flag: ' + flag);
      call && call.setException(err);
      self._close();
      return;
    }

    self.in.readBytes(size, function (err, buf) {
      var io = new DataInputBuffer(buf);

      var state = io.readInt(); // Read the state.  Currently unused.
      if (isError) {
        err = new RemoteException(io.readString(), io.readString());
        call && call.setException(err);
        if (!IGNORE_EXCEPTIONS[err.name]) {
          self._close(err);
        }
      } else {
        var obj = HbaseObjectWritable.readFields(io);
        debug('call#%s got %s instance', id, obj.declaredClass);
        call && call.setValue(obj.instance);
      }

      // RangeError: Maximum call stack size exceeded
      // self._nextResponse();
      process.nextTick(self._nextResponse.bind(self));
    });
  });

};

// impl HRegionInterface
Connection.prototype.call = function (method, parameters, rpcTimeout, callback) {
  if (typeof rpcTimeout === 'function') {
    callback = rpcTimeout;
    rpcTimeout = null;
  }
  rpcTimeout = rpcTimeout || this.rpcTimeout;
  var param = new Invocation(method, parameters);
  var call = new Call(param, rpcTimeout);
  var self = this;
  var connectionCallId = self._callNums++;
  self.calls[call.id] = call;
  call.on('done', function (err, value) {
    if (err) {
      err.connection = self.name;
      if (err.message.indexOf(err.connection) < 0) {
        err.message += ' (' + err.connection + ', ' + connectionCallId + ':' + self._callNums + ')';
      }
    }
    callback(err, value);
  }); // TODO: handle MAX Number error response, should close the connection.

  if (self.closed) {
    return this._handleClose();
  }

  call.on('timeout', function () {
    // remove the call
    delete self.calls[call.id];
  });
  debug('%s: sent call#%s request data', self.name, call.id);
  this.sendParam(call);
};

/**
 * Return all the data for the row that matches <i>row</i> exactly,
 * or the one that immediately preceeds it.
 *
 * @param regionName region name
 * @param row row key
 * @param family Column family to look for row in.
 * @return map of values
 * @throws IOException e
 */
Connection.prototype.getClosestRowBefore = function (regionName, row, family, rpcTimeout, callback) {
  this.call('getClosestRowBefore', [regionName, row, family], rpcTimeout, callback);
};

/**
 * Return protocol version corresponding to protocol interface.
 *
 * @param protocol The classname of the protocol interface
 * @param clientVersion The version of the protocol that the client speaks
 * @return the version that the server will speak
 * @throws IOException if any IO error occurs
 */
Connection.prototype.getProtocolVersion = function (protocol, clientVersion, rpcTimeout, callback) {
  protocol = protocol || HConstants.PROTOCOL;
  clientVersion = clientVersion || HConstants.CLIENT_VERSION;
  if (!(clientVersion instanceof Long)) {
    clientVersion = Long.fromNumber(clientVersion);
  }
  this.call('getProtocolVersion', [protocol, clientVersion], rpcTimeout, callback);
};

/**
 * Perform Get operation.
 * @param regionName name of region to get from
 * @param get Get operation
 * @return Result
 * @throws IOException e
 */
Connection.prototype.get = function (regionName, get, callback) {
  this.call('get', [regionName, get], callback);
};

/**
 * Put data into the specified region if check passes
 * @param regionName region name
 * @param wrapped args
 * @throws IOException e
 */
Connection.prototype.checkAndPut = function (regionName, o, callback) {
  var a = [regionName, o.getRow(), o.getFamily(), o.getQualifier(), o.getValue(), o.getPut()];
  this.call('checkAndPut', a, callback);
};

/**
 * Put data into the specified region
 * @param regionName region name
 * @param put the data to be put
 * @throws IOException e
 */
Connection.prototype.put = function (regionName, put, callback) {
  this.call('put', [regionName, put], callback);
};

/**
 * Delete data from the specified region
 * @param regionName region name
 * @param put the data to be put
 * @throws IOException e
 */
Connection.prototype.delete = function (regionName, del, callback) {
  this.call('delete', [regionName, del], callback);
};

/**
 * Method used for doing multiple actions(Deletes, Gets and Puts) in one call
 * @param {MultiAction} multi
 * @return MultiResult
 * @throws IOException
 */
Connection.prototype.multi = function (multi, callback) {
  this.call('multi', [multi], callback);
};

/**
 * Opens a remote scanner with a RowFilter.
 *
 * @param regionName name of region to scan
 * @param scan configured scan object
 * @return scannerId scanner identifier used in other calls
 * @throws IOException e
 */
Connection.prototype.openScanner = function (regionName, scan, callback) {
  this.call('openScanner', [regionName, scan], callback);
};

/**
 * Get the next set of values
 *
 * @param scannerId clientId passed to openScanner
 * @return map of values; returns null if no results.
 * @throws IOException e
 */
// Connection.prototype.next = function (scannerId, callback) {
//   this.call('next', [scannerId], callback);
// };

/**
 * Get the next set of values
 *
 * @param scannerId clientId passed to openScanner
 * @param [numberOfRows] the number of rows to fetch
 * @return Array of Results (map of values); array is empty if done with this
 * region and null if we are NOT to go to the next region (happens when a
 * filter rules that the scan is done).
 * @throws IOException e
 */
Connection.prototype.nextResult = function (scannerId, numberOfRows, callback) {
  if (typeof numberOfRows === 'function') {
    callback = numberOfRows;
    numberOfRows = null;
  }
  var params = [scannerId];
  if (numberOfRows) {
    params.push(numberOfRows);
  }
  this.call('next', params, callback);
};

/**
 * Close a scanner
 *
 * @param scannerId the scanner id returned by openScanner
 * @throws IOException e
 */
Connection.prototype.closeScanner = function (scannerId, callback) {
  this.call('close', [scannerId], callback);
};

Connection.Call_Counter = 0;


/** A call waiting for a value. */
function Call(param, timeout) {
  EventEmitter.call(this);
  this.id = Connection.Call_Counter++; // call id
  this.param = param; // parameter
  this.value = null; // value, null if error
  this.error = null; // exception, null if value
  this.done = false; // true when call is done
  this.startTime = Date.now();

  this.timeout = timeout;
  if (timeout && timeout > 0) {
    this.timer = setTimeout(this._handleTimeout.bind(this), timeout);
  }
}

util.inherits(Call, EventEmitter);

Call.prototype._handleTimeout = function () {
  var err = new errors.RemoteCallTimeoutException(this.timeout + ' ms timeout (call#' + this.id + ')');
  this.setException(err);
  this.emit('timeout');
};

/** Indicate when the call is complete and the
 * value or error are available.  Notifies by default.  */
Call.prototype.callComplete = function () {
  if (this.timer) {
    clearTimeout(this.timer);
    this.timer = null;
  }
  if (this.done) {
    // if done before, do not emit done again
    return;
  }
  this.done = true;
  if (debug.enabled) {
    debug('call#%d use %d ms', this.id, Date.now() - this.startTime);
  }
  this.emit('done', this.error, this.value);
};

/** Set the exception when there is an error.
 * Notify the caller the call is done.
 *
 * @param error exception thrown by the call; either local or remote
 */
Call.prototype.setException = function (err) {
  debug('call#%s error: %s', this.id, err.message);
  this.error = err;
  this.callComplete();
};

/** Set the return value when there is no error.
 * Notify the caller the call is done.
 *
 * @param value return value of the call.
 */
Call.prototype.setValue = function (value) {
  this.value = value;
  this.callComplete();
};

Call.prototype.getStartTime = function () {
  return this.startTime;
};


module.exports = Connection;
