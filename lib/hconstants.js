/*!
 * node-hbase-client - lib/hconstants.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Long = require('long');
var Bytes = require('./util/bytes');

var CONST = {};

CONST.PROTOCOL = 'org.apache.hadoop.hbase.ipc.HRegionInterface';
CONST.CLIENT_VERSION = Long.fromNumber(29);

/**
 * Timestamp to use when we want to refer to the latest cell.
 * This is the timestamp sent by clients when no timestamp is specified on
 * commit.
 */
CONST.LATEST_TIMESTAMP = Long.MAX_VALUE;
/**
 * Timestamp to use when we want to refer to the oldest cell.
 */
CONST.OLDEST_TIMESTAMP = Long.MIN_VALUE;
/**
 * LATEST_TIMESTAMP in bytes form
 */
CONST.LATEST_TIMESTAMP_BYTES = Bytes.toBytes(CONST.LATEST_TIMESTAMP);
/** The root table's name.*/
CONST.ROOT_TABLE_NAME = Bytes.toBytes("-ROOT-");
CONST.ROOT_TABLE_NAME.__name__ = '-ROOT-';

/** The META table's name. */
CONST.META_TABLE_NAME = Bytes.toBytes(".META.");
CONST.META_TABLE_NAME.__name__ = '.META.';

/** delimiter used between portions of a region name */
CONST.META_ROW_DELIMITER = ',';

/** The catalog family as a string*/
CONST.CATALOG_FAMILY_STR = "info";

/** The catalog family */
CONST.CATALOG_FAMILY = Bytes.toBytes(CONST.CATALOG_FAMILY_STR);

/** The regioninfo column qualifier */
CONST.REGIONINFO_QUALIFIER = Bytes.toBytes("regioninfo");

/** The server column qualifier */
CONST.SERVER_QUALIFIER = Bytes.toBytes("server");

/** The startcode column qualifier */
CONST.STARTCODE_QUALIFIER = Bytes.toBytes("serverstartcode");

/** The lower-half split region column qualifier */
CONST.SPLITA_QUALIFIER = Bytes.toBytes("splitA");

/** The upper-half split region column qualifier */
CONST.SPLITB_QUALIFIER = Bytes.toBytes("splitB");

/**
 * The meta table version column qualifier.
 * We keep current version of the meta table in this column in <code>-ROOT-</code>
 * table: i.e. in the 'info:v' column.
 */
CONST.META_VERSION_QUALIFIER = Bytes.toBytes("v");

/**
 * The current version of the meta table.
 * Before this the meta had HTableDescriptor serialized into the HRegionInfo;
 * i.e. pre-hbase 0.92.  There was no META_VERSION column in the root table
 * in this case.  The presence of a version and its value being zero indicates
 * meta is up-to-date.
 */
CONST.META_VERSION = 0;

/** long constant for zero */
// CONST.ZERO_L = Long.valueOf(0L);
CONST.NINES = "99999999999999";
CONST.ZEROES = "00000000000000";

// Other constants

/**
 * An empty instance.
 */
CONST.EMPTY_BYTE_ARRAY = new Buffer(0);

/**
 * Used by scanners, etc when they want to start at the beginning of a region
 */
CONST.EMPTY_START_ROW = CONST.EMPTY_BYTE_ARRAY;

/**
 * Last row in a table.
 */
CONST.EMPTY_END_ROW = CONST.EMPTY_START_ROW;

/**
* Used by scanners and others when they're trying to detect the end of a
* table
*/
CONST.LAST_ROW = CONST.EMPTY_BYTE_ARRAY;

/**
 * Parameter name for client prefetch limit, used as the maximum number of regions
 * info that will be prefetched.
 */
// public static String HBASE_CLIENT_PREFETCH_LIMIT = "hbase.client.prefetch.limit";

/**
 * Default value of {@link #HBASE_CLIENT_PREFETCH_LIMIT}.
 */
CONST.DEFAULT_HBASE_CLIENT_PREFETCH_LIMIT = 10;

/**
 * Parameter name for number of rows that will be fetched when calling next on
 * a scanner if it is not served from memory. Higher caching values will
 * enable faster scanners but will eat up more memory and some calls of next
 * may take longer and longer times when the cache is empty.
 */
CONST.HBASE_META_SCANNER_CACHING = "hbase.meta.scanner.caching";

/**
 * Default value of {@link #HBASE_META_SCANNER_CACHING}.
 */
CONST.DEFAULT_HBASE_META_SCANNER_CACHING = 100;

/**
 * Parameter name for maximum retries, used as maximum for all retryable
 * operations such as fetching of the root region from root region server,
 * getting a cell's value, starting a row update, etc.
 */
CONST.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER = 10;

/**
 * timeout for each RPC
 */
CONST.DEFAULT_HBASE_RPC_TIMEOUT = 30000; // 60000;

CONST.DEFAULT_PING_INTERVAL = 30000;

module.exports = CONST;
