/**!
 * node-hbase-client - lib/client.js
 *
 * Copyright(c) Alibaba Group Holding Limited.
 * MIT Licensed
 *
 * Authors:
 *   苏千 <suqian.yf@alibaba-inc.com> (http://fengmk2.github.com)
 */

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase:client');
var EventEmitter = require('events').EventEmitter;
var ZooKeeperWatcher = require('zookeeper-watcher');
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
var HRegionInfo = require('./hregion_info');
var HRegionLocation = require('./hregion_location');

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
  this.rpcTimeout = options.rpcTimeout || HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
  this.in = null;
  this.out = null;
  this.socket = null;
  this.logger = options.logger || console;
  options.zookeeperRoot = options.zookeeperRoot || "/hbase";
  if (options.zookeeper && typeof options.zookeeper.quorum === 'string') {
    options.zookeeperHosts = options.zookeeper.quorum.split(SERVERNAME_SEPARATOR);
  }
  this.zk = new ZooKeeperWatcher({
    hosts: options.zookeeperHosts,
    root: options.zookeeperRoot,
    logger: this.logger,
  });

  this.zkStart = 'init';
  this.rootRegionZKPath = options.rootRegionZKPath || '/root-region-server';
  this.numLocateRegionRetries = options.numLocateRegionRetries || 10;
  // tablename: [region1, region2, ...],
  this.cachedRegionLocations = {};
  // {hostname:port: server, ...}
  this.servers = {};
  this.serversLength = 0;

  // @wision: It makes sure that you prefetch the table only once at the time
  // (if called multiple times, it stores callbacks and awaits emit).
  this._prefetchRegionCacheList = {};

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
  this.maxActionRetries = options.maxActionRetries || 3;

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
  this._action('get', tableName, get, true, 0, callback);
};

Client.prototype.checkAndPut = function (tableName, row, family, qualifier, value, put, callback) {
  var _row = {
    getRow: function() {
      return row;
    },
    getFamily: function() { return family; },
    getQualifier: function() { return qualifier; },
    getValue: function() { return value; },
    getPut: function() { return put; }
  };
  this._action('checkAndPut', tableName, _row, callback);
};

Client.prototype.put = function (tableName, put, callback) {
  this._action('put', tableName, put, true, 0, callback);
};

Client.prototype.delete = function (tableName, del, callback) {
  this._action('delete', tableName, del, true, 0, callback);
};


Client.prototype.getScanner = function (tableName, scan, callback) {
  this._action('openScanner', tableName, scan, true, 0, function (err, scannerId, server) {
    if (err) {
      return callback(err);
    }
    var scanner = new Scanner(server, scannerId);
    callback(null, scanner);
  });
};

var _CACHE_TABLES = {};

function isRetryException(err) {
  var errName = err.name.toLowerCase();
  return errName.indexOf('org.apache.hadoop.hbase.') >= 0
    || errName.indexOf('offline') >= 0
    || errName.indexOf('noserver') >= 0
    || errName.indexOf('notserving') >= 0;
}

Client.prototype._action = function (method, tableName, obj, useCache, retry, callback) {
  retry = retry || 0;
  debug('action %s, useCache: %s, retry: %s', method, useCache, retry);
  if (!Buffer.isBuffer(tableName)) {
    tableName = _CACHE_TABLES[tableName] || new Buffer(tableName);
    _CACHE_TABLES[tableName] = tableName;
  }
  var row = obj.getRow();
  var self = this;
  self.locateRegion(tableName, row, useCache, function (err, location) {
    if (err || !location) {
      return callback(err);
    }

    self.getRegionConnection(location.getHostname(), location.getPort(), function (err, server) {
      if (err) {
        return callback(err);
      }

      server[method](location.getRegionInfo().getRegionName(), obj, function (err, value) {
        // org.apache.hadoop.hbase.regionserver.WrongRegionException retry
        if (err && isRetryException(err)) {
          retry++;
          self.logger.warn('[%s] [worker:%s] %s', Date(), process.pid, err.stack);
          if (retry <= self.maxActionRetries) {
            // max retries
            self.logger.warn('[%s] [worker:%s] [%s] %s retries %s table row %s got wrong region: %s',
              Date(), process.pid, method, retry, tableName, row.toString(), location.toString());
            return utility.setImmediate(self._action.bind(self, method, tableName, obj, false, retry, callback));
          }
        }

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
Client.prototype.processBatch = function (tableName, workingList, useCache, retry, callback) {
  var self = this;
  var actionsByServer = {};

  // remove empty
  var requestObjects = [];
  for (var i = 0; i < workingList.length; i++) {
    var item = workingList[i];
    if (!item) {
      continue;
    }
    requestObjects.push(item);
  }

  if (requestObjects.length === 0) {
    return callback(null, []);
  }

  var ep = EventProxy.create();
  ep.fail(callback);
  ep.after('multi_action', workingList.length, function () {
    // step 2: make the requests
    var requestSize = Object.keys(actionsByServer).length;
    if (debug.enabled) {
      debug('multi_action: Caculate regionServer: %d %d \n %s',
        workingList.length, requestSize, Object.keys(actionsByServer).join('\n'));
    }

    var results = [];
    var totalExceptionCount = 0;
    var retryException = null;
    ep.after('request_done', requestSize, function () {
      debug('processBatch got %d results, including %d exceptions',
        results.length, totalExceptionCount);
      if (retryException) {
        // org.apache.hadoop.hbase.regionserver.WrongRegionException retry
        retry++;
        if (retry <= self.maxActionRetries) {
          self.logger.warn('[%s] [worker:%s] %s', Date(), process.pid, retryException.message);
          self.logger.warn('[%s] [worker:%s] processBatch %s retries on table %s',
            Date(), process.pid, retry, tableName);
          return utility.setImmediate(self.processBatch.bind(self, tableName, workingList, false, retry, callback));
        }
        // max retry
        return callback(retryException, results);
      }
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
            // if (debug.enabled) {
            //   debug('Failures for region: %s, removing from cache', regionName);
            // }
            continue;
          }
          var idx = pair.getFirst();
          var result = pair.getSecond();
          results[idx] = result;

          // if (result.name === 'org.apache.hadoop.hbase.NotServingRegionException') {
          //   delete self.cachedRegionLocations[Bytes.mapKey(regionName.split(',')[0])];
          // }

          if (result instanceof Error) {
            totalExceptionCount++;
            if (!retryException && isRetryException(result)) {
              retryException = result;
              if (debug.enabled) {
                debug('Failures for region: %s, may be removing from cache, error: %s',
                  regionName, retryException.message);
              }
            }
          }
        }
      }
    }

    // step 2: make the requests
    function makeRequest(location, multiAction) {
      var hostname = location.getHostname();
      var port = location.getPort();
      self.getRegionConnection(hostname, port, ep.done(function (server) {
        server.multi(multiAction, ep.done(function (value) {
          processResult(value);
          ep.emit('request_done');
        }));
      }));
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
    self.locateRegion(tableName, row.getRow(), useCache, ep.done(function (loc) {
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

      actions[1].add(regionInfo, action);
      ep.emit('multi_action');
    }));
  }

  for (var i = 0; i < requestObjects.length; i++) {
    buildDataStructs(requestObjects[i], i);
  }
};

/**
 * Get a row with columns.
 *
 * @param {String|Buffer} tableName
 * @param {String|Buffer} row
 * @param {Array} [columns], column name, format: 'family:qualifier'.
 *   if `columns` not set or null or '*', will return all columns. like `select *`.
 *   e.g.: `['cf1:name', 'cf2:age', 'cf1:title']`
 * @param {Function(err, data)} callback
 */
Client.prototype.getRow = function (tableName, row, columns, callback) {
  var get = new Get(row);
  if (typeof columns === 'function') {
    callback = columns;
    columns = null;
  }
  if (Array.isArray(columns) && columns.length > 0) {
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
 * @param {Object} opts, e.g.: `{raw: true}`
 * @param {Function(err, data)} callback
 */
Client.prototype.mget = function (tableName, rows, columns, opts, callback) {
  if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }
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
  this.processBatch(tableName, workingList, true, 0, function (err, results) {
    if (err) {
      return callback(err);
    }
    for (var j = 0, len = results.length; j < len; j++) {
      var data = results[j];
      if (!data || data.constructor.name !== 'Result') {
        results[j] = null;
        // TODO: what's this?
        continue;
      }
      var r = null;
      if (opts.raw) {
        results[j] = data;
      } else {
        var kvs = data.raw();
        if (kvs.length > 0) {
          r = {};
          for (var i = 0; i < kvs.length; i++) {
            var kv = kvs[i];
            r[kv.getFamily().toString() + ':' + kv.getQualifier().toString()] = kv.getValue();
          }
        }
        results[j] = r;
      }
    }
    callback(null, results);
  });
};

/**
 * put rows into table
 * @param  {String|Buffer} tableName
 * @param  {Array} rows [{row: 'aaabbbcccddd', 'f:name': 'xa', 'f:age': 1}]
 */
Client.prototype.mput = function (tableName, rows, callback) {
  if (!Array.isArray(rows)) {
    return callback('Input rows must be an array.');
  }
  for (var i = 0, len = rows.length; i < len; i++) {
    if (!(rows[i] instanceof Put) && !rows[i].row) {
      return callback('Object must have property row or be instance of Put.');
    }
  }
  var workingList = [];
  for (var i = 0, len = rows.length; i < len; i++) {
    var data = rows[i];
    var put = null;
    if (data instanceof Put) {
      put = data;
    } else {
      put = new Put(data.row, data.ts);
      for (var k in data) {
        if (k === 'row' || k === 'ts') {
          continue;
        }
        var col = k.split(':');
        put.add(col[0], col[1], data[k]);
      }
    }
    workingList.push(put);
  }
  this.processBatch(tableName, workingList, true, 0, callback);
};

/**
 * delete rows from table
 * @param  {String|Buffer}  tableName
 * @param  {Array} rows ['aabbcc']
 */
Client.prototype.mdelete = function (tableName, rows, callback) {
  var workingList = [];
  for (var i = 0, len = rows.length; i < len; i++) {
    var row = rows[i];
    var del = new Delete(row);
    workingList.push(del);
  }
  this.processBatch(tableName, workingList, true, 0, callback);
};

/**
 * multi upsert. Put for values, Delete for null cells
 *
 * @param {String|Buffer} tableName
 * @param {Array} rows ['aabbcc']
 */
Client.prototype.mupsert = function (tableName, rows, callback) {
  var workingList = [];

  for (var i = 0, len = rows.length; i < len; i++) {
    var data = rows[i];
    var del = null;
    var put = null;

    for (var key in data) {
      if (key === 'row' || key === 'ts') {
        continue;
      }
      var col = key.split(':');
      if (data[key] === null) { //content of qualifier is null
        if (del === null) { // lazy create
          del = new Delete(data.row, data.ts);
        }
        del.deleteColumn(col[0], col[1]);
      } else {
        if (put === null) { // lazy create
          put = new Put(data.row, data.ts);
        }
        put.add(col[0], col[1], data[key]);
      }
    }

    if (put !== null) {
      workingList.push(put);
    }
    if (del !== null) {
      workingList.push(del);
    }
  }

  this.processBatch(tableName, workingList, true, 0, callback);
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
 * @param {Boolean} useCache default is true
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

  self.zkStart = 'starting';
  self.zk.once('connected', function (err) {
    if (err) {
      self.zkStart = 'error';
      self.logger.warn('[%s] [worker:%s] [hbase-client] zookeeper connect error: %s',
        Date(), process.pid, err.stack);
      return self.emit('ready', err);
    }
    self.zk.unWatch(self.rootRegionZKPath);

    self.zk.watch(self.rootRegionZKPath, function (err, value, zstat) {
      var firstStart = self.zkStart !== 'done';
      if (err) {
        self.logger.warn('[%s] [worker:%s] [hbase-client] zookeeper watch error: %s',
          Date(), process.pid, err.stack);
        if (firstStart) {
          // only first start fail will emit ready event
          self.zkStart = 'error';
          self.emit('ready', err);
        }
        return;
      }

      self.zkStart = 'done';
      var rootServer = self.createServerName(value);
      var oldRootServer = self.rootServerName;
      self.rootServerName = rootServer;
      self.logger.warn('[%s] [worker:%s] [hbase-client] zookeeper start done, got new root %s, old %s',
        Date(), process.pid, rootServer.servername, oldRootServer ? oldRootServer.servername : null);
      if (firstStart) {
        // only first start success will emit ready event
        self.emit('ready');
      }
    });
  });
};

Client.prototype._syncRootRegion = function () {
  var self = this;
  self.zk.get(self.rootRegionZKPath, function (err, value, zstat) {
    if (err) {
      return self.logger.error(err);
    }
    var rootServer = self.createServerName(value);
    var oldRootServer = self.rootServerName;
    self.rootServerName = rootServer;
    self.logger.warn('[%s] [worker:%s] [hbase-client] zookeeper start done, got new root %s, old %s',
      Date(), process.pid, rootServer.servername, oldRootServer ? oldRootServer.servername : null);
  });
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

  if (!hostAndPort) {
    return null;
  }

  // Instantiate the location
  var item = hostAndPort.split(':');
  var hostname = item[0];
  var port = parseInt(item[1], 10);

  var location = new HRegionLocation(regionInfo, hostname, port);
  return location;
};

/**
  * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
  * info that contains the table and row we're seeking.
  */
Client.prototype.locateRegionInMeta = function (parentTable, tableName, row, useCache, callback, tries) {
  debug('locateRegionInMeta, useCache: %s, tries: %s', useCache, tries);

  if (!Buffer.isBuffer(tableName)) {
    tableName = Bytes.toBytes(tableName);
  }

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
      debug('locateRegion %s:%s from %s(%s), current root: %s',
        tableName.toString(), metaKey.toString(),
        parentTable.toString(),
        metaLocation.getHostnamePort(),
        self.rootServerName.servername);
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
          if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
            // locate meta error, try to reload root region meta
            // make sure root change and zookeeper not working
            self._syncRootRegion();
          }

          // Only relocate the parent region if necessary
          if (isRetryException(err)) {
            tries = tries || 0;
            if (tries >= self.numRetries) {
              return callback(err);
            }
            tries++;

            self.logger.warn('[%s] [worker:%s] getClosestRowBefore error: %s', Date(), process.pid, err.stack);
            self.logger.warn('[%s] [worker:%s] %s retries to locateRegion: %s',
              Date(), process.pid, tries, metaKey.toString());
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
            Bytes.toString(parentTable) + ", row=" + regionInfoRow));
        }

        var regionInfo = location.regionInfo;

        // possible we got a region of a different table...
        if (!Bytes.equals(regionInfo.getTableName(), tableName)) {
          return callback(new TableNotFoundException("Table '" + Bytes.toString(tableName) +
            "' was not found, got: " + Bytes.toString(regionInfo.getTableName()) + "."));
        }
        if (regionInfo.isSplit()) {
          return callback(new errors.RegionOfflineException("the only available region for"
            + " the required row is a split parent,"
            + " the daughters should be online soon: " + regionInfo));
        }
        if (regionInfo.isOffline()) {
          return callback(new errors.RegionOfflineException(
            "the region is offline, could be caused by a disable table call: " + regionInfo));
        }

        self.cacheLocation(tableName, location);

        // If the parent table is META, we may want to pre-fetch some
        // region info into the global region cache for this table.
        if (Bytes.equals(parentTable, HConstants.META_TABLE_NAME)) {
          var eventName = Buffer.concat([tableName, regionInfo.startKey]).toString();
          // make sure only one request for the same region prefetch
          if (self._prefetchRegionCacheList[eventName]) {
            self.once(eventName, callback);
            return;
          }

          self._prefetchRegionCacheList[eventName] = true;
          self.prefetchRegionCache(tableName, regionInfo.startKey, function (err, count) {
            self.logger.warn('[%s, startRow:%s] prefetchRegionCache %d locations',
              tableName.toString(), regionInfo.startKey, count);
            if (err) {
              self.logger.warn('[prefetchRegionCache] error: %s', err.stack);
            }

            delete self._prefetchRegionCacheList[eventName];
            self.emit(eventName, null, location);
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

  var timer = null;

  var handleConnectionError = function handleConnectionError(err) {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }

    self.clearCachedLocationForServer(rsName);
    delete self.servers[rsName];
    self.serversLength--;

    // avoid 'close' and 'connect' event emit.
    server.removeAllListeners();
    server.close();

    debug(err.message);
    self.emit(readyEvent, err);
  };

  // handle connect timeout
  timer = setTimeout(function () {
    var err = new errors.ConnectionConnectTimeoutException(rsName + ' connect timeout, ' + self.rpcTimeout + ' ms');
    handleConnectionError(err);
  }, self.rpcTimeout);

  server.once('connect', function () {
    clearTimeout(timer);
    timer = null;

    // should getProtocolVersion() first to check version
    server.getProtocolVersion(null, null, function (err, version) {
      server.state = 'ready';

      if (err) {
        return self.emit(readyEvent, err);
      }
      version = version.toNumber();
      debug('%s connected, protocol: %s, total %d connections', rsName, version, self.serversLength);

      self.emit(readyEvent, null, server);
    });
  });

  server.once('connectError', handleConnectionError);

  // TODO: connection always emit close event?
  server.once('close', self._handleConnectionClose.bind(self, rsName));
};

Client.prototype._handleConnectionClose = function (rsName) {
  // TODO: connection always emit close event?
  delete this.servers[rsName];
  this.serversLength--;

  // clean relation regions cache
  this.clearCachedLocationForServer(rsName);
  debug('%s closed, total %d connections', rsName, this.serversLength);
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
  var address = {
    hostname: items[0],
    port: parseInt(items[1], 10),
    startcode: Number(items[2]),
    servername: servername
  };

  // servername maybe: "xxxxx.cm6:60020"
  if (isNaN(address.port) && address.hostname.indexOf(':')) {
    items = address.hostname.split(':');
    address.hostname = items[0];
    address.port = parseInt(items[1], 10);
  }

  debug('createServerName(%j) => %j', servername, address);
  return address;
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
      debug('clearRegionCache %s: %d cache regions, cache key: %s',
        tableName.toString(),
        this.cachedRegionLocations[key] ? this.cachedRegionLocations[key].length : 0,
        key);
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
          if (!location || !Bytes.equals(location.regionInfo.tableName, tableName)) {
            self.logger.warn('[%s] [worker:%s] [hbase-client] prefetchRegionCache %s empty',
              Date(), process.pid, Bytes.toString(tableName));
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
