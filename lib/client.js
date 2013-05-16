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
var Connection = require('./connection');
var errors = require('./errors');
var TableNotFoundException = errors.TableNotFoundException;
var IOException = errors.IOException;
var Get = require('./get');
var Put = require('./put');
var Delete = require('./delete');
var Scanner = require('./scanner');
var Scan = require('./scan');
var utility = require('utility');
var MultiResponse = require('./multi_response');
var EventProxy = require('eventproxy');
var Pair = require('./pair');
var MultiAction = require('./multi_action');
var Action = require('./action');

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
  this.logger = options.logger;
  this.zkStart = 'init';
  this.numLocateRegionRetries = options.numLocateRegionRetries || 10;
  // tablename: [region1, region2, ...],
  this.cachedRegionLocations = {};
  // {hostname:port: server, ...}
  this.servers = {};
  this.serversLength = 0;

  // The presence of a server in the map implies it's likely that there is an
  // entry in cachedRegionLocations that map to this server; but the absence
  // of a server in this map guarentees that there is no entry in cache that
  // maps to the absent server.
  this.cachedServers = {};

  // region cache prefetch is enabled by default. this set contains all
  // tables whose region cache prefetch are disabled.
  this.regionCachePrefetchDisabledTables = {};

  this.prefetchRegionLimit = options.prefetchRegionLimit || HConstants.DEFAULT_HBASE_CLIENT_PREFETCH_LIMIT;
  this.numRetries = options.numRetries || HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;

  this.ensureZookeeperTrackers(utility.noop);
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

Client.prototype.delete = function (tableName, del, callback) {
  this._action('delete', tableName, del, callback);
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

var _CACHE_TABLES = {};

Client.prototype._action = function (method, tableName, obj, callback) {
  if (!Buffer.isBuffer(tableName)) {
    tableName = _CACHE_TABLES[tableName] || new Buffer(tableName);
    _CACHE_TABLES[tableName] = tableName;
  }
  var row = obj.getRow();
  var self = this;
  self.locateRegion(tableName, row, true, function (err, location) {
    if (err || !location) {
      return callback(err);
    }

    self.getRegionConnection(location.getHostname(), location.getPort(), function (err, server) {
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
 * Parameterized batch processing, allowing varying return types for
 * different {@link Row} implementations.
 * @param {byte []} tableName
 */
Client.prototype.processBatch = function (tableName, workingList, callback) {
  var self = this;
  var ep = new EventProxy();
  ep.fail(callback);
  var actionsByServer = {};
  
  ep.after('multi_action', workingList.length, function () {
    // step 2: make the requests
    var requestSize = Object.keys(actionsByServer).length;
    if (debug.enabled) {
      debug('caculate regionServer: %d %d \n %s', workingList.length, Object.keys(actionsByServer).length, Object.keys(actionsByServer).join('\n'));
    }

    var retry = false;
    var results = [];
    var exceptions = [];
    ep.after('request_done', requestSize, function () {
      callback(null, results);
    });
     
    // step 4: identify failures and prep for a retry (if applicable).
    // TODO: retry for exceptions
    // step 3: collect the failures and successes and prepare for retry
    function processResult(value) {
      if (!value) {
        return;
      }
      for (var regionName in value.results) {
        var regionResults = value.results[regionName];
        for (var j = 0; j < regionResults.length; j++) {
          var pair = regionResults[j];
          if (!pair) {
            // if the first/only record is 'null' the entire region failed.
            if (debug.enabled) {
              debug('Failures for region: %s, removing from cache', regionName);
            }
            continue;
          }
          var idx = pair.getFirst();
          var result = pair.getSecond();
          if (result.constructor.name !== 'Result') {
            result[idx] = result;
            continue;
          }
          var r = null;
          var kvs = result.raw();
          if (kvs.length > 0) {
            r = {};
            for (var i = 0; i < kvs.length; i++) {
              var kv = kvs[i];
              r[kv.getFamily().toString() + ':' + kv.getQualifier().toString()] = kv.getValue().toString('utf-8');
            }
          }
          results[idx] = r;
        }
      }
    }
    
    // step 2: make the requests
    function makeRequest(location, mua) {
      self.getRegionConnection(location.getHostname(), location.getPort(), function (err, server) {
        if (err) {
          ep.emit('error', err);
          return;
        }
        server.multi(mua, function (err, value) {
          if (err) {
            ep.emit('error', err);
            return;
          }
          processResult(value);
          ep.emit('request_done');
        });
      });
    }

    for (var loc in actionsByServer) {
      var action = actionsByServer[loc];
      var location = action[0];
      var mua = action[1];
      makeRequest(location, mua);
    }
  });
  
  // step 1: break up into regionserver-sized chunks and build the data structs
  function buildDataStructs(row, i) {
    self.locateRegion(tableName, row.getRow(), true, function (err, loc) {
      if (err) {
        return ep.emit('error', err);
      }
      if (!loc) {
        // TODO: retry
        return ep.emit('multi_action');
      }
      var regionInfo = loc.getRegionInfo();
      var key = regionInfo.regionNameStr;
      var actions = actionsByServer[key];
      if (!actions) {
        actions = actionsByServer[key] = [loc, new MultiAction()];
      }
      var action = new Action(row, i);
      // lastServers[i] = loc;
      actions[1].add(regionInfo, action);
      ep.emit('multi_action');
    });
  }
  for (var i = 0; i < workingList.length; i++) {
    var row = workingList[i];
    if (row) {
      buildDataStructs(row, i);
    } else {
      ep.emit('multi_action');
    }
  }
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
 * Get rows with columns.
 * 
 * @param {String|Buffer} tableName
 * @param {List<String|Buffer>} row
 * @param {Array} columns, column name, format: 'family:qualifier'.
 *   e.g.: `['cf1:name', 'cf2:age', 'cf1:title']`
 * @param {Function(err, data)} callback
 */
Client.prototype.mget = function (tableName, rows, columns, callback) {
  var workingList = [];
  for (var j = 0; j < rows.length; j++) {
    var row = rows[j];
    var get = new Get(row);
    if (columns) {
      for (var i = 0; i < columns.length; i++) {
        var col = columns[i].split(':');
        get.addColumn(col[0], col[1]);
      }
    }
    workingList.push(get);
  }
  this.processBatch(tableName, workingList, callback);
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

/**
 * Delete a row from table.
 * 
 * @param {String|Buffer} tableName
 * @param {String|Buffer} row
 * @param {Function(err)} callback
 */
Client.prototype.deleteRow = function (tableName, row, callback) {
  var del = new Delete(row);
  this.delete(tableName, del, callback);
};

/**
 * Find the location of the region of <i>tableName</i> that <i>row</i>
 * lives in.
 * 
 * @param {Buffer|String} tableName, name of the table <i>row</i> is in
 * @param {Buffer|String} row, row key you're trying to find the region of
 * @param {Boolean} useCache
 * @param {Function(err, location)} callback
 *  - {HRegionLocation} location, that describes where to find the region in question
 */
Client.prototype.locateRegion = function (tableName, row, useCache, callback) {
  if (typeof useCache === 'function') {
    callback = useCache;
    useCache = true;
  }
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
      // debug('Got Root Server: %j', servername);
      callback(null, new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, servername.hostname, servername.port));
    } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      self.locateRegionInMeta(HConstants.ROOT_TABLE_NAME, tableName, row, useCache, callback);
    } else {
      // Region not in the cache - have to go to the meta RS
      self.locateRegionInMeta(HConstants.META_TABLE_NAME, tableName, row, useCache, callback);
    }
  });
};

/**
 * Find the location of the region of <i>tableName</i> that <i>row</i>
 * lives in, ignoring any value that might be in the cache.
 * 
 * @param tableName name of the table <i>row</i> is in
 * @param row row key you're trying to find the region of
 * @return HRegionLocation that describes where to find the region in
 * question
 * @throws IOException if a remote or network exception occurs
 */
Client.prototype.relocateRegion = function (tableName, row, callback) {
  this.locateRegion(tableName, row, false, callback);
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
  self.zk.once('connected', function (err) {
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
  // self.zk.start();
};

Client.prototype._storeRegionInfo = function (regionInfoRow) {
  var value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
  if (!value || value.length === 0) {
    return null;
  }

  // convert the row result into the HRegionLocation we need!
  var io = new DataInputBuffer(value);
  var regionInfo = new HRegionInfo();
  regionInfo.readFields(io);

  value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
  var hostAndPort = "";
  if (value !== null) {
    hostAndPort = Bytes.toString(value);
  }
  // if (!hostAndPort) {
  //   return callback(new errors.NoServerForRegionException("No server address listed " + 
  //     "in " + Bytes.toString(parentTable) + " for region " + 
  //     regionInfo.getRegionNameAsString() + " containing row " + Bytes.toStringBinary(row)));
  // }
  if (!hostAndPort) {
    return null;
  }

  // Instantiate the location
  var item = hostAndPort.split(':');
  var hostname = item[0];
  var port = parseInt(item[1], 10);
  if (debug.enabled) {
    debug('_storeRegionInfo [%s:%s] %s', hostname, port, regionInfo.toString());
  }
  
  var location = new HRegionLocation(regionInfo, hostname, port);
  return location;
};

/**
  * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
  * info that contains the table and row we're seeking.
  */
Client.prototype.locateRegionInMeta = function (parentTable, tableName, row, useCache, callback, tries) {
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

    self.getRegionConnection(metaLocation.getHostname(), metaLocation.getPort(), function (err, server) {
      if (err) {
        return callback(err);
      }

      // Check the cache again for a hit in case some other thread made the
      // same query while we were waiting on the lock. If not supposed to
      // be using the cache, delete any existing cached location so it won't
      // interfere.
      
      var location = null;
      if (useCache) {
        location = self.getCachedLocation(tableName, row);
        if (location) {
          return callback(null, location);
        }
      } else {
        self.deleteCachedLocation(tableName, row);
      }

      // Query the root or meta region for the location of the meta region
      server.getClosestRowBefore(metaLocation.getRegionInfo().getRegionName(), metaKey, HConstants.CATALOG_FAMILY, 
      function (err, regionInfoRow) {
        if (err) {
          var errName = err.name.toLowerCase();
          var errMsg = err.message.toLowerCase();
          // Only relocate the parent region if necessary
          if (errName.indexOf('offline') || errMsg.indexOf('offline') || 
              errName.indexOf('noserver') || errMsg.indexOf('noserver') ||
              errName.indexOf('notserving') || errMsg.indexOf('notserving')) {
            self.logger.warn(err.stack);

            tries = tries || 0;
            if (tries > self.numRetries) {
              return callback(err);
            }
            tries++;

            self.logger.warn('[' + new Date() + '] [WARNNING] %d retries to locateRegion: %s', tries, metaKey.toString());
            self.relocateRegion(parentTable, metaKey, function (err) {
              if (err) {
                return callback(err);
              }

              self.clearRegionCache(tableName);
              // try again
              self.locateRegionInMeta(parentTable, tableName, row, false, callback, tries);
            });
            return;
          }

          return callback(err);
        }

        if (regionInfoRow === null) {
          return callback(new TableNotFoundException("Table '" + Bytes.toString(tableName) + "' was not found"));
        }

        var location = self._storeRegionInfo(regionInfoRow);

        if (!location) {
          return callback(new IOException("HRegionInfo was null or empty in " + 
            Bytes.toString(parentTable) + ", row=" + regionInfoRow.toString()));
        }

        var regionInfo = location.regionInfo;

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

        // if (!hostAndPort) {
        //   return callback(new errors.NoServerForRegionException("No server address listed " + 
        //     "in " + Bytes.toString(parentTable) + " for region " + 
        //     regionInfo.getRegionNameAsString() + " containing row " + Bytes.toStringBinary(row)));
        // }

        self.cacheLocation(tableName, location);

        // If the parent table is META, we may want to pre-fetch some
        // region info into the global region cache for this table.
        if (Bytes.equals(parentTable, HConstants.META_TABLE_NAME)) {
          self.prefetchRegionCache(tableName, regionInfo.startKey, function (err, count) {
          // self.prefetchRegionCache(tableName, HConstants.EMPTY_START_ROW, function (err, count) {
            self.logger.warn('[%s, startRow:%s] prefetchRegionCache %d locations', tableName.toString(), regionInfo.startKey, count);
            if (err) {
              self.logger.warn('[prefetchRegionCache] error: %s', err.stack);
            }

            callback(null, location);
          });
        } else {
          callback(null, location);
        }
        
      });

    });

  });
};

/**
 * Get region connection.
 * 
 * @param {String} hostname
 * @param {Number} port
 * @param {Function(err, server)} callback
 */
Client.prototype.getRegionConnection = function (hostname, port, callback) {
  var server;
  var rsName = hostname + ':' + port;
  var self = this;
  // See if we already have a connection (common case)
  server = self.servers[rsName];
  var readyEvent = 'getRegionConnection:' + rsName + ':ready';
  if (server && server.state === 'ready') {
    debug('getRegionConnection from cache(%d), %s', self.serversLength, rsName);
    return callback(null, server);
  }

  // debug('watting `%s` event', readyEvent);
  self.once(readyEvent, callback);

  if (server) {
    return;
  }
  
  server = new Connection({
    host: hostname,
    port: port,
    rpcTimeout: self.rpcTimeout,
    logger: self.logger,
  });
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
      }
      version = version.toNumber();
      debug('Protocol: %s, %s emit', version, readyEvent);

      self.emit(readyEvent, null, server);
    });
  });

  server.once('close', function () {
    delete self.servers[rsName];
    self.serversLength--;

    // clean relation regions cache
    self.clearCachedLocationForServer(rsName);
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
 * Delete all cached entries of a table that maps to a specific location.
 *
 * @param hostnamePort
 */
Client.prototype.clearCachedLocationForServer = function (hostnamePort) {
  var deletedCount = 0;
  if (!this.cachedServers[hostnamePort]) {
    return;
  }

  for (var key in this.cachedRegionLocations) {
    var locations = this.cachedRegionLocations[key];
    var needs = [];
    var deleted = false;
    for (var i = 0; i < locations.length; i++) {
      var location = locations[i];
      if (location.getHostnamePort() === hostnamePort) {
        deletedCount++;
        deleted = true;
      } else {
        needs.push(location);
      }
    }
    if (deleted) {
      this.cachedRegionLocations[key] = needs;
    }
  }

  delete this.cachedServers[hostnamePort];
  this.logger.warn("Removed %d cached region locations that map to `%s`", deletedCount, hostnamePort);
};

/*
 * @param tableName
 * @return Map of cached locations for passed <code>tableName</code>
 */
Client.prototype.getTableLocations = function (tableName) {
  // find the map of cached locations for this table
  var key = tableName.__key;
  if (!key) {
    key = tableName.__key = Bytes.mapKey(tableName);
  }
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

  this.cachedServers[location.getHostnamePort()] = true;
  tableLocations.push(location);
};

Client.prototype.clearRegionCache = function (tableName) {
  if (tableName) {
    var key = tableName.__key;
    if (!key) {
      key = tableName.__key = Bytes.mapKey(tableName);
    }
    if (debug.enabled) {
      debug('clearRegionCache %s: %d', 
        tableName.toString(), this.cachedRegionLocations[key] ? this.cachedRegionLocations[key].length : 0);
    }
    this.cachedRegionLocations[key] = [];
  } else {
    this.cachedRegionLocations = {};
  }
  this.cachedServers = {};
};

/*
 * Search .META. for the HRegionLocation info that contains the table and
 * row we're seeking. It will prefetch certain number of regions info and
 * save them to the global region cache.
 */
Client.prototype.prefetchRegionCache = function (tableName, row, callback) {
  // Implement a new visitor for MetaScanner, and use it to walk through
  // the .META.
  var self = this;
  var startRow = HRegionInfo.createRegionName(tableName, row, HConstants.ZEROES, false);
  var scan = new Scan(startRow);
  scan.addFamily(HConstants.CATALOG_FAMILY);
  self.getScanner(HConstants.META_TABLE_NAME, scan, function (err, scanner) {
    var count = 0;
    var done = function (error) {
      if (scanner) {
        scanner.close(function () {
          callback(error, count);
        });
      } else {
        callback(error, count);
      }
    };

    if (err) {
      return done(err);
    }

    var next = function (numberOfRows) {
      scanner.next(numberOfRows, function (err, rows) {
        if (err) {
          return done(err);
        }
        if (!rows || rows.length === 0) {
          return done();
        }
        var closed = false;
        rows.forEach(function (regionInfoRow) {
          var location = self._storeRegionInfo(regionInfoRow);
          if (!Bytes.equals(location.regionInfo.tableName, tableName)) {
            closed = true;
            return false;
          }

          var regionInfo = location.regionInfo;

          if (regionInfo.isSplit()) {
            return;
          }
          if (regionInfo.isOffline()) {
            return;
          }

          count++;
          self.cacheLocation(tableName, location);
        });
        
        if (closed) {
          return done();
        }
        
        next(numberOfRows);
      });
    };
    next(10);
  });
};


module.exports = Client;
