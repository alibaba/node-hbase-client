/*!
 * node-hbase-client - lib/connection.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase-client');
var Text = require('./text');
var ResponseFlag = require('./ipc/response_flag');
var DataInputBuffer = require('./data_input_buffer');
var RemoteException = require('./errors').RemoteException;

var HEADER = new Buffer("hrpc", "utf8");
var CURRENT_VERSION = 3;
var PING_CALL_ID = -1;

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

ConnectionHeader.prototype = {
  // readFields: function (io) {
  //   this.protocol = Text.readString(in);
  //   if (!this.protocol) {
  //     this.protocol = null;
  //   }
  // },

  write: function (out) {
    Text.writeString(out, this.protocol === null ? "" : this.protocol);
  },

  getProtocol: function () {
    return this.protocol;
  },

  getUser: function () {
    return null;
  },

  toString: function () {
    return this.protocol;
  },
};


/** Thread that reads responses and notifies callers.  Each connection owns a
 * socket connected to a remote address.  Calls are multiplexed through this
 * socket: responses may be delivered out of order. */
function Connection(remoteId) {
  this.header = null; // connection header
  this.remoteId = null;
  this.socket = null; // connected socket
  this.in;
  this.out;

  // currently active calls
  this.calls = {}; // new ConcurrentSkipListMap<Integer, Call>();
  this.lastActivity = new AtomicLong();// last I/O activity time
  this.shouldCloseConnection = new AtomicBoolean(); // indicate if the connection is closed
  this.closeException; // close reason

  // if (remoteId.getAddress().isUnresolved()) {
  //   throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
  // }
  this.remoteId = remoteId;
  var ticket = remoteId.getTicket();
  var protocol = remoteId.getProtocol();

  this.header = new ConnectionHeader(protocol === null ? null : protocol.getName(), ticket);

  // this.setName("IPC Client (" + socketFactory.hashCode() + ") connection to " + remoteId.getAddress().toString()
  //     + ((ticket == null) ? " from an unknown user" : (" from " + ticket.getName())));
  this.setDaemon(true);

}

Connection.prototype = {

  /** Update lastActivity with the current time. */
  touch: function () {
    lastActivity.set(Date.now());
  },

  /**
   * Add a call to this connection's call queue and notify
   * a listener; synchronized.
   * Returns false if called during shutdown.
   * @param call to add
   * @return true if the call was added.
   */
  // protected synchronized boolean addCall(Call call) {
  //   if (shouldCloseConnection.get())
  //     return false;
  //   calls.put(call.id, call);
  //   notify();
  //   return true;
  // }

  /** This class sends a ping to the remote side when timeout on
   * reading. If no failure is detected, it retries until at least
   * a byte is read.
   */
  
  // protected class PingInputStream extends FilterInputStream {
  //   /* constructor */
  //   protected PingInputStream(InputStream in) {
  //     super(in);
  //   }

  //   /* Process timeout exception
  //    * if the connection is not going to be closed, send a ping.
  //    * otherwise, throw the timeout exception.
  //    */
  //   private void handleTimeout(SocketTimeoutException e) throws IOException {
  //     if (shouldCloseConnection.get() || !running.get() || remoteId.rpcTimeout > 0) {
  //       throw e;
  //     }
  //     sendPing();
  //   }

  //   /** Read a byte from the stream.
  //    * Send a ping if timeout on read. Retries if no failure is detected
  //    * until a byte is read.
  //    * @throws IOException for any IO problem other than socket timeout
  //    */
  //   @Override
  //   public int read() throws IOException {
  //     do {
  //       try {
  //         return super.read();
  //       } catch (SocketTimeoutException e) {
  //         handleTimeout(e);
  //       }
  //     } while (true);
  //   }

  //   /** Read bytes into a buffer starting from offset <code>off</code>
  //    * Send a ping if timeout on read. Retries if no failure is detected
  //    * until a byte is read.
  //    *
  //    * @return the total number of bytes read; -1 if the connection is closed.
  //    */
  //   @Override
  //   public int read(byte[] buf, int off, int len) throws IOException {
  //     do {
  //       try {
  //         return super.read(buf, off, len);
  //       } catch (SocketTimeoutException e) {
  //         handleTimeout(e);
  //       }
  //     } while (true);
  //   }
  // }

  setupConnection: function () {
    var ioFailures = 0;
    var timeoutFailures = 0;
    // while (true) {
    //   try {
    //     this.socket = socketFactory.createSocket();
    //     this.socket.setTcpNoDelay(tcpNoDelay);
    //     this.socket.setKeepAlive(tcpKeepAlive);
    //     // connection time out is 20s
    //     NetUtils.connect(this.socket, remoteId.getAddress(), getSocketTimeout(conf));
    //     if (remoteId.rpcTimeout > 0) {
    //       pingInterval = remoteId.rpcTimeout; // overwrite pingInterval
    //     }
    //     this.socket.setSoTimeout(pingInterval);
    //     return;
    //   } catch (SocketTimeoutException toe) {
    //     /* The max number of retries is 45,
    //      * which amounts to 20s*45 = 15 minutes retries.
    //      */
    //     handleConnectionFailure(timeoutFailures++, maxRetries, toe);
    //   } catch (IOException ie) {
    //     handleConnectionFailure(ioFailures++, maxRetries, ie);
    //   }
    // }
  },

  /** Connect to the server and set up the I/O streams. It then sends
   * a header to the server and starts
   * the connection thread that waits for responses.
   * @throws java.io.IOException e
   */
  setupIOstreams: function () {

    // if (this.socket != null || shouldCloseConnection.get()) {
    //   return;
    // }
    debug("Connecting to " + this.remoteId);
    this.setupConnection();
    // this.in = new DataInputStream(new BufferedInputStream(new PingInputStream(NetUtils.getInputStream(socket))));
    // this.out = new DataOutputStream(new BufferedOutputStream(NetUtils.getOutputStream(socket)));
    this.writeHeader();

    // update last activity time
    this.touch();

    // start the receiver thread after the socket connection has been set up
    this.start();

    // try {
    //   if (LOG.isDebugEnabled()) {
    //     LOG.debug("Connecting to " + remoteId);
    //   }
    //   setupConnection();
    //   this.in = new DataInputStream(new BufferedInputStream(new PingInputStream(NetUtils.getInputStream(socket))));
    //   this.out = new DataOutputStream(new BufferedOutputStream(NetUtils.getOutputStream(socket)));
    //   writeHeader();

    //   // update last activity time
    //   touch();

    //   // start the receiver thread after the socket connection has been set up
    //   start();
    // } catch (IOException e) {
    //   markClosed(e);
    //   close();

    //   throw e;
    // }
  },

  // protected void closeConnection() {
  //   // close the current connection
  //   if (socket != null) {
  //     try {
  //       socket.close();
  //     } catch (IOException e) {
  //       LOG.warn("Not able to close a socket", e);
  //     }
  //   }
  //   // set socket to null so that the next call to setupIOstreams
  //   // can start the process of connect all over again.
  //   socket = null;
  // },

  /**
   *  Handle connection failures
   *
   * If the current number of retries is equal to the max number of retries,
   * stop retrying and throw the exception; Otherwise backoff N seconds and
   * try connecting again.
   *
   * This Method is only called from inside setupIOstreams(), which is
   * synchronized. Hence the sleep is synchronized; the locks will be retained.
   *
   * @param curRetries current number of retries
   * @param maxRetries max number of retries allowed
   * @param ioe failure reason
   * @throws IOException if max number of retries is reached
   */
  // private void handleConnectionFailure(int curRetries, int maxRetries, IOException ioe) throws IOException {

  //   closeConnection();

  //   // throw the exception if the maximum number of retries is reached
  //   if (curRetries >= maxRetries) {
  //     throw ioe;
  //   }

  //   // otherwise back off and retry
  //   try {
  //     Thread.sleep(failureSleep);
  //   } catch (InterruptedException ignored) {
  //   }

  //   LOG.info("Retrying connect to server: " + remoteId.getAddress() + " after sleeping " + failureSleep
  //       + "ms. Already tried " + curRetries + " time(s).");
  // }

  /* Write the header for each connection
   * Out is not synchronized because only the first thread does this.
   */
  writeHeader: function () {
    this.out.write(HEADER);
    this.out.writeByte(CURRENT_VERSION);
    //When there are more fields we can have ConnectionHeader Writable.
    var buf = new DataOutputBuffer();
    header.write(buf);

    var bufLen = buf.getLength();
    this.out.writeInt(bufLen);
    this.out.write(buf.getData(), 0, bufLen);
  },

  /* wait till someone signals us to start reading RPC response or
   * it is idle too long, it is marked as to be closed,
   * or the client is marked as not running.
   *
   * Return true if it is time to read a response; false otherwise.
   */
  //@SuppressWarnings({ "ThrowableInstanceNeverThrown" })
  // protected synchronized boolean waitForWork() {
  //   if (calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
  //     long timeout = maxIdleTime - (System.currentTimeMillis() - lastActivity.get());
  //     if (timeout > 0) {
  //       try {
  //         wait(timeout);
  //       } catch (InterruptedException ignored) {
  //       }
  //     }
  //   }

  //   if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
  //     return true;
  //   } else if (shouldCloseConnection.get()) {
  //     return false;
  //   } else if (calls.isEmpty()) { // idle connection closed or stopped
  //     markClosed(null);
  //     return false;
  //   } else { // get stopped but there are still pending requests
  //     markClosed((IOException) new IOException().initCause(new InterruptedException()));
  //     return false;
  //   }
  // }

  // public InetSocketAddress getRemoteAddress() {
  //   return remoteId.getAddress();
  // }

  /* Send a ping to the server if the time elapsed
   * since last I/O activity is equal to or greater than the ping interval
   */
  // protected synchronized void sendPing() throws IOException {
  //   long curTime = System.currentTimeMillis();
  //   if (curTime - lastActivity.get() >= pingInterval) {
  //     lastActivity.set(curTime);
  //     //noinspection SynchronizeOnNonFinalField
  //     synchronized (this.out) {
  //       out.writeInt(PING_CALL_ID);
  //       out.flush();
  //     }
  //   }
  // },

  @Override
  public void run() {
    if (LOG.isDebugEnabled())
      LOG.debug(getName() + ": starting, having connections " + connections.size());

    try {
      while (waitForWork()) {//wait here for work - read or close connection
        receiveResponse();
      }
    } catch (Throwable t) {
      LOG.warn("Unexpected exception receiving call responses", t);
      markClosed(new IOException("Unexpected exception receiving call responses", t));
    }

    close();

    if (LOG.isDebugEnabled())
      LOG.debug(getName() + ": stopped, remaining connections " + connections.size());
  }

  /* Initiates a call by sending the parameter to the remote server.
   * Note: this is not called from the Connection thread, but by other
   * threads.
   */
  sendParam: function (call) {
    // if (shouldCloseConnection.get()) {
    //   return;
    // }

    // For serializing the data to be written.

    debug(getName() + " sending #" + call.id);

    var d = new DataOutputBuffer();
    d.writeInt(0xdeadbeef); // placeholder for data length
    d.writeInt(call.id);
    call.param.write(d);
    var data = d.getData();
    var dataLength = d.getLength();
    // fill in the placeholder
    Bytes.putInt(data, 0, dataLength - 4);
    //noinspection SynchronizeOnNonFinalField
    this.out.write(data, 0, dataLength);
    // } catch (IOException e) {
    //   markClosed(e);
    // } finally {
    //   //the buffer is just an in-memory buffer, but it is still polite to
    //   // close early
    //   IOUtils.closeStream(d);
    // }
  },

  /* Receive a response.
   * Because only one receiver, so no synchronization on in.
   */
  receiveResponse: function () {
    // if (shouldCloseConnection.get()) {
    //   return;
    // }
    var self = this;
    self.touch();

    // See HBaseServer.Call.setResponse for where we write out the response.
    // It writes the call.id (int), a flag byte, then optionally the length
    // of the response (int) followed by data.

    self.in.readFields([
      {name: 'id', method: 'readInt'},
      {name: 'flag', method: 'readByte'},
      {name: 'size', method: 'readInt'},
    ], function (err, data) [
      if (err) {

      }
      
      // Read the call id.
      var id = data.id;

      debug('%s got value #%s', getName(), id);
      var call = self.calls.remove(id);

      // Read the flag byte
      var flag = data.flag;
      var isError = ResponseFlag.isError(flag);
      if (!ResponseFlag.isLength(flag)) {
        var err = new RemoteException('RemoteException', 'missing data length packet, flag: ' + flag);
        call.setException(err);
        return;
      }

      var size = data.size;
      self.in.readBytes(size, function (err, buf) {
        var io = new DataInputBuffer(buf);

        // var state = data.size; // Read the state.  Currently unused.
        if (isError) {
          var err = new RemoteException(io.readString(), io.readString());
          call.setException(err);
          return;
        }

        Writable value = ReflectionUtils.newInstance(valueClass, conf);
        value.readFields(io); // read value
        // it's possible that this call may have been cleaned up due to a RPC
        // timeout, so check if it still exists before setting the value.
        // if (call != null) {
        //   call.setValue(value);
        // }
        call.setValue(value);

      });
    ]);

    

    //  catch (IOException e) {
    //   if (e instanceof SocketTimeoutException && remoteId.rpcTimeout > 0) {
    //     // Clean up open calls but don't treat this as a fatal condition,
    //     // since we expect certain responses to not make it by the specified
    //     // {@link ConnectionId#rpcTimeout}.
    //     closeException = e;
    //   } else {
    //     // Since the server did not respond within the default ping interval
    //     // time, treat this as a fatal condition and close this connection
    //     markClosed(e);
    //   }
    // } finally {
    //   if (remoteId.rpcTimeout > 0) {
    //     cleanupCalls(remoteId.rpcTimeout);
    //   }
    // }
  },

  // protected synchronized void markClosed(IOException e) {
  //   if (shouldCloseConnection.compareAndSet(false, true)) {
  //     closeException = e;
  //     notifyAll();
  //   }
  // }

  /** Close the connection. */
  // protected synchronized void close() {
  //   if (!shouldCloseConnection.get()) {
  //     LOG.error("The connection is not in the closed state");
  //     return;
  //   }

  //   // release the resources
  //   // first thing to do;take the connection out of the connection list
  //   synchronized (connections) {
  //     connections.remove(remoteId, this);
  //   }

  //   // close the streams and therefore the socket
  //   IOUtils.closeStream(out);
  //   IOUtils.closeStream(in);

  //   // clean up all calls
  //   if (closeException == null) {
  //     if (!calls.isEmpty()) {
  //       LOG.warn("A connection is closed for no cause and calls are not empty");

  //       // clean up calls anyway
  //       closeException = new IOException("Unexpected closed connection");
  //       cleanupCalls();
  //     }
  //   } else {
  //     // log the info
  //     if (LOG.isDebugEnabled()) {
  //       LOG.debug("closing ipc connection to " + remoteId.address + ": " + closeException.getMessage(),
  //           closeException);
  //     }

  //     // cleanup calls
  //     cleanupCalls();
  //   }
  //   if (LOG.isDebugEnabled())
  //     LOG.debug(getName() + ": closed");
  // }

  /* Cleanup all calls and mark them as done */
  // protected void cleanupCalls() {
  //   cleanupCalls(0);
  // }

  // protected void cleanupCalls(long rpcTimeout) {
  //   Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
  //   while (itor.hasNext()) {
  //     Call c = itor.next().getValue();
  //     long waitTime = System.currentTimeMillis() - c.getStartTime();
  //     if (waitTime >= rpcTimeout) {
  //       if (this.closeException == null) {
  //         // There may be no exception in the case that there are many calls
  //         // being multiplexed over this connection and these are succeeding
  //         // fine while this Call object is taking a long time to finish
  //         // over on the server; e.g. I just asked the regionserver to bulk
  //         // open 3k regions or its a big fat multiput into a heavily-loaded
  //         // server (Perhaps this only happens at the extremes?)
  //         this.closeException = new CallTimeoutException("Call id=" + c.id + ", waitTime=" + waitTime
  //             + ", rpcTimetout=" + rpcTimeout);
  //       }
  //       c.setException(this.closeException);
  //       synchronized (c) {
  //         c.notifyAll();
  //       }
  //       itor.remove();
  //     } else {
  //       break;
  //     }
  //   }
  //   try {
  //     if (!calls.isEmpty()) {
  //       Call firstCall = calls.get(calls.firstKey());
  //       long maxWaitTime = System.currentTimeMillis() - firstCall.getStartTime();
  //       if (maxWaitTime < rpcTimeout) {
  //         rpcTimeout -= maxWaitTime;
  //       }
  //     }
  //     if (!shouldCloseConnection.get()) {
  //       closeException = null;
  //       if (socket != null) {
  //         socket.setSoTimeout((int) rpcTimeout);
  //       }
  //     }
  //   } catch (SocketException e) {
  //     LOG.debug("Couldn't lower timeout, which may result in longer than expected calls");
  //   }
  // }
};


module.exports = Connection;
