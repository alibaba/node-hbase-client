/*!
 * node-hbase-client - lib/client.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase:client');
var EventEmitter = require('events').EventEmitter;
var HRegionInfo = require('./hregion_info');
var HRegionLocation = require('./hregion_location');
var ZooKeeperWatcher = require('zookeeper-watcher');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Bytes = require('./util/bytes');
var DataOutputBuffer = require('./data_output_buffer');
var DataInputBuffer = require('./data_input_buffer');
var HConstants = require('./hconstants');
var ConnectionId = require('./ipc/connection_id');
var Connection = require('./connection');
var errors = require('./errors');
var TableNotFoundException = errors.TableNotFoundException;
var IOException = errors.IOException;
var Get = require('./get');
var Put = require('./put');
var Scanner = require('./scanner');

/**
 * This character is used as separator between server hostname, port and
 * startcode.
 */
var SERVERNAME_SEPARATOR = ",";

function Client(options) {
  if (!(this instanceof Client)) {
    return new Client(options);
  }
  EventEmitter.call(this);

  this.in = null;
  this.out = null;
  this.socket = null;
  this.zk = new ZooKeeperWatcher({
    hosts: options.zookeeperHosts,
    root: options.zookeeperRoot,
    logger: options.logger,
  });
  this.zkStart = 'init';
  this.numLocateRegionRetries = options.numLocateRegionRetries || 10;
  // tablename: [region1, region2, ...],
  this.cachedRegionLocations = {};
  // {hostname:port: server, ...}
  this.servers = {};
  this.serversLength = 0;

  // region cache prefetch is enabled by default. this set contains all
  // tables whose region cache prefetch are disabled.
  this.regionCachePrefetchDisabledTables = {};

  this.prefetchRegionLimit = options.prefetchRegionLimit || HConstants.DEFAULT_HBASE_CLIENT_PREFETCH_LIMIT;

}

util.inherits(Client, EventEmitter);

Client.create = function (options) {
  return new Client(options);
};


// The metadata attached to each piece of data has the
// format:
//   <magic> 1-byte constant
//   <id length> 4-byte big-endian integer (length of next field)
//   <id> identifier corresponding uniquely to this process
// It is prepended to the data supplied by the user.

// the magic number is to be backward compatible
var MAGIC = 255;
var MAGIC_SIZE = Bytes.SIZEOF_BYTE;
var ID_LENGTH_OFFSET = MAGIC_SIZE;
var ID_LENGTH_SIZE = Bytes.SIZEOF_INT;
function removeMetaData(data) {
  if (data === null || data.length === 0) {
    return data;
  }
  // check the magic data; to be backward compatible
  var magic = data[0];
  if (magic !== MAGIC) {
    return data;
  }

  var idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
  var dataLength = data.length - MAGIC_SIZE - ID_LENGTH_SIZE - idLength;
  var dataOffset = MAGIC_SIZE + ID_LENGTH_SIZE + idLength;

  return data.slice(dataOffset, dataOffset + dataLength);
}
Client.removeMetaData = removeMetaData;

/**
 * Extracts certain cells from a given row.
 * @param get The object that specifies what data to fetch and from which row.
 * @return The data coming from the specified row, if it exists.  If the row
 * specified doesn't exist, the {@link Result} instance returned won't
 * contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
 * @throws IOException if a remote or network exception occurs.
 * @since 0.20.0
 */
Client.prototype.get = function (tableName, get, callback) {
  this._action('get', tableName, get, callback);
};

Client.prototype.put = function (tableName, put, callback) {
  this._action('put', tableName, put, callback);
};

Client.prototype.getScanner = function (tableName, scan, callback) {
  this._action('openScanner', tableName, scan, function (err, scannerId, server) {
    if (err) {
      return callback(err);
    }
    var scanner = new Scanner(server, scannerId);
    callback(null, scanner);
  });
};

Client.prototype._action = function (method, tableName, obj, callback) {
  if (!Buffer.isBuffer(tableName)) {
    tableName = new Buffer(tableName);
  }
  var row = obj.getRow();
  var self = this;
  self.locateRegion(tableName, row, true, function (err, location) {
    if (err || !location) {
      return callback(err);
    }

    self.getHRegionConnection(location.getHostname(), location.getPort(), function (err, server) {
      if (err) {
        return callback(err);
      }

      server[method](location.getRegionInfo().getRegionName(), obj, function (err, value) {
        callback(err, value, server);
      });

    });
  });
};

/**
 * Get a row with columns.
 * 
 * @param {String|Buffer} tableName
 * @param {String|Buffer} row
 * @param {Array} columns, column name, format: 'family:qualifier'.
 *   e.g.: `['cf1:name', 'cf2:age', 'cf1:title']`
 * @param {Function(err, data)} callback
 */
Client.prototype.getRow = function (tableName, row, columns, callback) {
  var get = new Get(row);
  if (columns) {
    for (var i = 0; i < columns.length; i++) {
      var col = columns[i].split(':');
      get.addColumn(col[0], col[1]);
    }
  }
  this.get(tableName, get, function (err, result) {
    if (err || !result) {
      return callback(err, result);
    }
    var r = null;
    var kvs = result.raw();
    if (kvs.length > 0) {
      r = {};
      for (var i = 0; i < kvs.length; i++) {
        var kv = kvs[i];
        r[kv.getFamily().toString() + ':' + kv.getQualifier().toString()] = kv.getValue();
      }
    }
    callback(null, r);
  });
};

/**
 * Put a row to table.
 * 
 * @param {String|Buffer} tableName
 * @param {String|Buffer} row
 * @param {Object} data, e.g.: `{'f1:name': 'foo', 'f1:age': '18'}`
 * @param {Function(err)} callback
 */
Client.prototype.putRow = function (tableName, row, data, callback) {
  var put = new Put(row);
  for (var k in data) {
    // 'f:q'
    var col = k.split(':');
    put.add(col[0], col[1], data[k]);
  }
  this.put(tableName, put, callback);
};

Client.prototype.locateRegion = function (tableName, row, useCache, callback) {
  if (!Buffer.isBuffer(tableName)) {
    tableName = Bytes.toBytes(tableName);
  }
  if (row === null) {
    row = new Buffer(0);
  }
  if (!Buffer.isBuffer(row)) {
    row = Bytes.toBytes(row);
  }

  var self = this;
  self.ensureZookeeperTrackers(function (err) {
    // TODO: handle err
    if (err) {
      return callback(err);
    }

    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      var servername = self.rootServerName;
      debug('Got Root Server: %j', servername);
      return callback(null, new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, servername.hostname, servername.port));
    } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      self.locateRegionInMeta(HConstants.ROOT_TABLE_NAME, tableName, row, useCache, callback);
    } else {
      // Region not in the cache - have to go to the meta RS
      self.locateRegionInMeta(HConstants.META_TABLE_NAME, tableName, row, useCache, callback);
    }
  });
};

Client.prototype.ensureZookeeperTrackers = function (callback) {
  var self = this;
  if (self.zkStart === 'done') {
    return callback();
  }
  
  self.once('ready', callback);

  if (self.zkStart === 'starting') {
    return;
  }

  var rootPath = '/root-region-server';
  self.zkStart = 'starting';
  self.zk.once('started', function (err) {
    if (err) {
      self.zkStart = 'error';
      return self.emit('ready', err);
    }
    self.zk.unWatch(rootPath);

    self.zk.watch(rootPath, function (err, value, zstat) {
      if (err) {
        self.zkStart = 'error';
        return self.emit('ready', err);
      }

      self.zkStart = 'done';
      self.rootServerName = self.createServerName(value);
      debug('zookeeper Start, got root %j', self.rootServerName);
      // console.log('root-region-server value is %j', self.rootServerName);
      // TODO: not emit ready when path change
      self.emit('ready');
    });
  });
  self.zk.start();
};

/**
  * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
  * info that contains the table and row we're seeking.
  */
Client.prototype.locateRegionInMeta = function (parentTable, tableName, row, useCache, callback) {
  var location;
  // If we are supposed to be using the cache, look in the cache to see if
  // we already have the region.
  if (useCache) {
    location = this.getCachedLocation(tableName, row);
    if (location) {
      return callback(null, location);
    }
  }

  // build the key of the meta region we should be looking for.
  // the extra 9's on the end are necessary to allow "exact" matches
  // without knowing the precise region names.
  var metaKey = HRegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
  
  var self = this;
  var metaLocation = null;
  // locate the root or meta region
  self.locateRegion(parentTable, metaKey, false, function (err, metaLocation) {
    if (err) {
      return callback(err);
    }

    if (!metaLocation) {
      // TODO: retries
      return callback();
    }

    if (debug.enabled) {
      debug('locateRegion %s from %s, got %s', 
        tableName.toString(), parentTable.toString(), metaLocation.getHostnamePort());
    }

    self.getHRegionConnection(metaLocation.getHostname(), metaLocation.getPort(), function (err, server) {
      if (err) {
        return callback(err);
      }

      var getRegionInfo = function () {
        // Check the cache again for a hit in case some other thread made the
        // same query while we were waiting on the lock. If not supposed to
        // be using the cache, delete any existing cached location so it won't
        // interfere.
        
        var location = null;
        if (useCache) {
          location = self.getCachedLocation(tableName, row);
          if (location !== null) {
            return callback(null, location);
          }
        } 
        // else {
        //   // self.deleteCachedLocation(tableName, row);
        // }

        // Query the root or meta region for the location of the meta region
        server.getClosestRowBefore(metaLocation.getRegionInfo().getRegionName(), metaKey, 
            HConstants.CATALOG_FAMILY, function (err, regionInfoRow) {
          if (err) {
            return callback(err);
          }

          if (regionInfoRow === null) {
            return callback(new TableNotFoundException("Table '" + Bytes.toString(tableName) + "' was not found"));
          }

          var value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
          if (!value || value.length === 0) {
            return callback(new IOException("HRegionInfo was null or empty in " + 
              Bytes.toString(parentTable) + ", row=" + regionInfoRow.toString()));
          }

          // convert the row result into the HRegionLocation we need!
          var io = new DataInputBuffer(value);
          var regionInfo = new HRegionInfo();
          regionInfo.readFields(io);
          // possible we got a region of a different table...
          if (!Bytes.equals(regionInfo.getTableName(), tableName)) {
            return callback(new TableNotFoundException("Table '" + Bytes.toString(tableName) + 
              "' was not found, got: " + Bytes.toString(regionInfo.getTableName()) + "."));
          }
          if (regionInfo.isSplit()) {
            return callback(new errors.RegionOfflineException("the only available region for" + 
              " the required row is a split parent," + 
              " the daughters should be online soon: " + regionInfo.getRegionNameAsString()));
          }
          if (regionInfo.isOffline()) {
            return callback(new errors.RegionOfflineException(
              "the region is offline, could be caused by a disable table call: " + 
              regionInfo.getRegionNameAsString()));
          }

          value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
          var hostAndPort = "";
          if (value !== null) {
            hostAndPort = Bytes.toString(value);
          }
          if (!hostAndPort) {
            return callback(new errors.NoServerForRegionException("No server address listed " + 
              "in " + Bytes.toString(parentTable) + " for region " + 
              regionInfo.getRegionNameAsString() + " containing row " + Bytes.toStringBinary(row)));
          }

          // Instantiate the location
          var item = hostAndPort.split(':');
          var hostname = item[0];
          var port = parseInt(item[1], 10);
          if (debug.enabled) {
            debug('getClosestRowBefore [%s:%s] %s', hostname, port, regionInfo.toString());
          }
          
          var location = new HRegionLocation(regionInfo, hostname, port);
          self.cacheLocation(tableName, location);
          callback(null, location);

        });
      };

      // If the parent table is META, we may want to pre-fetch some
      // region info into the global region cache for this table.
      // if (Bytes.equals(parentTable, HConstants.META_TABLE_NAME)) {
      //   self.prefetchRegionCache(tableName, row, function (err) {
      //     if (err) {
      //       return callback(err);
      //     }
      //     getRegionInfo();
      //   });
      // } else {
      //   getRegionInfo();
      // }
      getRegionInfo();

    });

  });
};

/**
 * Either the passed <code>isa</code> is null or <code>hostname</code>
 * can be but not both.
 * @param hostname
 * @param port
 * @param isa
 * @param master
 * @return Proxy.
 * @throws IOException
 */
Client.prototype.getHRegionConnection = function (hostname, port, callback, master) {
  var server;
  var rsName = hostname + ':' + port;
  var self = this;
  // See if we already have a connection (common case)
  server = self.servers[rsName];
  var readyEvent = 'getHRegionConnection:' + rsName + ':ready';
  if (server && server.state === 'ready') {
    debug('getHRegionConnection from cache(%d), %s', self.serversLength, rsName);
    return callback(null, server);
  }

  debug('watting `%s` event', readyEvent);
  self.once(readyEvent, callback);

  if (server) {
    return;
  }
  
  // Only create isa when we need to.
  // InetSocketAddress address = isa != null ? isa : new InetSocketAddress(hostname, port);
  // definitely a cache miss. establish an RPC for this RS
  // server = (HRegionInterface) HBaseRPC.waitForProxy(serverInterfaceClass, HRegionInterface.VERSION,
  //     address, this.conf, this.maxRPCAttempts, this.rpcTimeout, this.rpcTimeout);
  // this.servers.put(Addressing.createHostAndPortStr(address.getHostName(), address.getPort()), server);
  var remoteId = new ConnectionId({
    host: hostname,
    port: port,
  }, null, null, self.rpcTimeout);
  
  server = new Connection(remoteId);
  server.state = 'connecting';
  // cache server
  self.servers[rsName] = server;
  self.serversLength++;

  server.on('connect', function () {
    // should getProtocolVersion() first to check version
    server.getProtocolVersion(null, null, function (err, version) {
      server.state = 'ready';

      if (err) {
        return self.emit(readyEvent, err);
        //return callback(err);
      }
      version = version.toNumber();
      debug('Protocol: %s, %s emit', version, readyEvent);

      // callback(null, server);
      self.emit(readyEvent, null, server);
    });
  });

  server.once('close', function () {
    delete self.servers[rsName];
    self.serversLength--;
  });

  // TODO: handle `timeout` event 
  // http://nodejs.org/docs/v0.8.23/api/net.html#net_socket_settimeout_timeout_callback
};

/**
 * Delete a cached location
 * @param tableName tableName
 * @param row
 */
Client.prototype.deleteCachedLocation = function (tableName, row) {
  var key = Bytes.mapKey(tableName);
  var tableLocations = this.cachedRegionLocations[key];
  if (!tableLocations || !tableLocations.length) {
    return;
  }

  // start to examine the cache. we can only do cache actions
  // if there's something in the cache for this table.
  var needs = [];
  for (var i = 0; i < tableLocations.length; i++) {
    var location = tableLocations[i];
    var r = Bytes.compareTo(row, location.regionInfo.startKey);
    if (r < 0) {
      needs.push(location);
      continue;
    }
  }

  this.cachedRegionLocations[key] = needs;

  // if (!tableLocations.isEmpty()) {
  //   HRegionLocation rl = getCachedLocation(tableName, row);
  //   if (rl != null) {
  //     tableLocations.remove(rl.getRegionInfo().getStartKey());
  //     if (LOG.isDebugEnabled()) {
  //       LOG.debug("Removed " + rl.getRegionInfo().getRegionNameAsString() + " for tableName="
  //           + Bytes.toString(tableName) + " from cache " + "because of " + Bytes.toStringBinary(row));
  //     }
  //   }
  // }
};

/*
 * Search the cache for a location that fits our table and row key.
 * Return null if no suitable region is located. TODO: synchronization note
 *
 * <p>TODO: This method during writing consumes 15% of CPU doing lookup
 * into the Soft Reference SortedMap.  Improve.
 *
 * @param tableName
 * @param row
 * @return Null or region location found in cache.
 */
Client.prototype.getCachedLocation = function (tableName, row) {
  var tableLocations = this.getTableLocations(tableName);

  // start to examine the cache. we can only do cache actions
  // if there's something in the cache for this table.
  if (!tableLocations.length) {
    return null;
  }

  for (var i = 0; i < tableLocations.length; i++) {
    var location = tableLocations[i];
    var startKey = location.regionInfo.startKey;
    var endKey = location.regionInfo.endKey;
    var r = Bytes.compareTo(row, startKey);
    if (r >= 0) {
      if (endKey.length === 0 || Bytes.compareTo(endKey, row) > 0) {
        if (debug.enabled) {
          debug('getCachedLocation hit(%d: %d): get location(%s)', 
            tableLocations.length, i, location.toString());
        }
        
        return location;
      }
    }
  }

  debug('getCachedLocation miss(%d)', tableLocations.length);
  return null;

  // var possibleRegion = tableLocations.get(row);
  // if (possibleRegion !== null) {
  //   return possibleRegion;
  // }

  // possibleRegion = tableLocations.lowerValueByKey(row);
  // if (possibleRegion === null) {
  //   return null;
  // }

  // // make sure that the end key is greater than the row we're looking
  // // for, otherwise the row actually belongs in the next region, not
  // // this one. the exception case is when the endkey is
  // // HConstants.EMPTY_END_ROW, signifying that the region we're
  // // checking is actually the last region in the table.
  // var endKey = possibleRegion.getRegionInfo().getEndKey();
  // if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW)
  //     || KeyValue.getRowComparator(tableName).compareRows(endKey, 0, endKey.length, row, 0, row.length) > 0) {
  //   return possibleRegion;
  // }

  // // Passed all the way through, so we got nothin - complete cache miss
  // return null;
};

Client.prototype.createServerName = function (data) {
  data = removeMetaData(data);
  var servername = Bytes.toString(data);
  var items = servername.split(SERVERNAME_SEPARATOR);
  return {
    hostname: items[0],
    port: parseInt(items[1], 10),
    startcode: Number(items[2]),
  };
};

/*
 * @param tableName
 * @return Map of cached locations for passed <code>tableName</code>
 */
Client.prototype.getTableLocations = function (tableName) {
  // find the map of cached locations for this table
  var key = Bytes.mapKey(tableName);
  var result = this.cachedRegionLocations[key];
  if (!result) {
    this.cachedRegionLocations[key] = result = [];
  }
  return result;
};

/*
 * Put a newly discovered HRegionLocation into the cache.
 */
Client.prototype.cacheLocation = function (tableName, location) {
  // TODO: remove location when it split and offline
  var tableLocations = this.getTableLocations(tableName);
  for (var i = 0; i < tableLocations.length; i++) {
    var o = tableLocations[i];
    if (location.regionInfo.compareTo(o.regionInfo) === 0) {
      // if location exists, do not cache it.
      return;
    }
  }

  tableLocations.push(location);
};

/*
 * Search .META. for the HRegionLocation info that contains the table and
 * row we're seeking. It will prefetch certain number of regions info and
 * save them to the global region cache.
 */
Client.prototype.prefetchRegionCache = function (tableName, row, callback) {
  // Implement a new visitor for MetaScanner, and use it to walk through
  // the .META.
  callback();
  // MetaScannerVisitor visitor = new MetaScannerVisitor() {
  //   public boolean processRow(Result result) throws IOException {
  //     try {
  //       byte[] value = result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
  //       HRegionInfo regionInfo = null;

  //       if (value != null) {
  //         // convert the row result into the HRegionLocation we need!
  //         regionInfo = Writables.getHRegionInfo(value);

  //         // possible we got a region of a different table...
  //         if (!Bytes.equals(regionInfo.getTableName(), tableName)) {
  //           return false; // stop scanning
  //         }
  //         if (regionInfo.isOffline()) {
  //           // don't cache offline regions
  //           return true;
  //         }
  //         value = result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
  //         if (value == null) {
  //           return true; // don't cache it
  //         }
  //         final String hostAndPort = Bytes.toString(value);
  //         String hostname = Addressing.parseHostname(hostAndPort);
  //         int port = Addressing.parsePort(hostAndPort);
  //         value = result.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
  //         // instantiate the location
  //         HRegionLocation loc = new HRegionLocation(regionInfo, hostname, port);
  //         // cache this meta entry
  //         cacheLocation(tableName, loc);
  //       }
  //       return true;
  //     } catch (RuntimeException e) {
  //       throw new IOException(e);
  //     }
  //   }
  // };
  // try {
  //   // pre-fetch certain number of regions info at region cache.
  //   MetaScanner.metaScan(conf, visitor, tableName, row, this.prefetchRegionLimit);
  // } catch (IOException e) {
  //   LOG.warn("Encountered problems when prefetch META table: ", e);
  // }
};


module.exports = Client;
