
1.6.1 / 2018-01-02
==================

**fixes**
  * [[`27886f2`](http://github.com/alibaba/node-hbase-client/commit/27886f2dfb99bd49e91cc20b3b2a889bb2c5420c)] - fix: let scanner scan through regions (#101) (Khaidi Chu <<i@2333.moe>>)

**others**
  * [[`3f46ff3`](http://github.com/alibaba/node-hbase-client/commit/3f46ff39ca66de14414642df0aeb9e3a8aa4a040)] - doc: fix history (#100) (zōng yǔ <<gxcsoccer@users.noreply.github.com>>)

1.6.0 / 2017-11-24
==================

**features**
  * [[`bf83163`](https://github.com/alibaba/node-hbase-client.git/commit/bf831639474f7f2ef267899bc93e57665b0cefa1)] - feat: add client.checkAnd* support (#99) (Khaidi Chu <<i@2333.moe>>)
**others**
  * [[`7680b52`](https://github.com/alibaba/node-hbase-client.git/commit/7680b52659d2785972e68cc8c18afc542707c701)] - test: can't pass test because some incompatible code in mocha (#98) (Khaidi Chu <<i@2333.moe>>)

1.5.0 / 2017-08-16
==================

**features**
  * [[`f175bd7`](http://github.com/alibaba/node-hbase-client/commit/f175bd7d82b04ec4a0e4b2d7da34e64e1f14d5c5)] - feat: make zk as a parameter (#94) (zōng yǔ <<gxcsoccer@users.noreply.github.com>>)

1.4.0 / 2015-08-10
==================

 * feat: Added RowFilter support to enable easier scans for specific group of rows.
 * doc: Change url and name of hbase protobuf implementation

1.3.0 / 2015-03-05
==================

 * Add SingleColumnValueFilter (@hase1031)
 * Fixes HbaseObjectWritable when clazz not in CLASS_TO_CODE (@hase1031)
 * Add Comparators for filter (@hase1031)
 * add 0.96 help

1.2.2 / 2015-01-20
==================

 * Parse latest version of hbase 0.94.x from the web
 * Use push to add item to array (@wision)

1.2.1 / 2014-11-25
==================

 * Call clearCachedLocationForServer while handling connection error (@wision)

1.2.0 / 2014-10-02
==================

 * Fix tests & use Travis (@wision)
 * multi upsert function (@tzolkincz)

1.1.1 / 2014-09-05
==================

 * Fix mput object check (@wision)

1.1.0 / 2014-09-05
==================

 * Add ColumnPrefixFilter & ColumnRangeFilter (@wision)

1.0.0 / 2014-08-28
==================

 * Allow mput array of Put objects
 * result add getRow()

0.5.0 / 2014-04-03
==================

 * no need to delete cache region locations manually
 * Raw option for mget (@wision)

0.4.5 / 2014-04-03
==================

 * make test with custom config file
 * Use config for tests + small fixes (@wision)
 * Fix ConnectionClosedException for newer nodejs version (@wision)
 * Add a CRUD get started

0.4.4 / 2014-03-13 
==================

  * add missing RegionOfflineException to errors

0.4.3 / 2014-03-12
==================

  * fix _storeRegionInfo() return null TypeError
  * RegionInfo getRegionNameAsString typeError bug fix

0.4.2 / 2014-03-12
==================

  * processBatch() will retry when region server return org.apache.hadoop.hbase.* Exception fixed #55

0.4.1 / 2014-02-27
==================

  * use dns.lookup to resolve domain. fixed #54
  * key only filter test case value is length buffer int

0.4.0 / 2014-02-26
==================

  * add missing filter name
  * Support simple filter, like only scan row key.
  * add null item

0.3.5 / 2014-02-20
==================

  * dont change input data on mput()

0.3.4 / 2014-02-20
==================

  * Support HBase 0.94.16 fixed #36
  * mv benchmark to docs/. fixed #45

0.3.3 / 2014-02-17
==================

  * connection refused error instead of connection timeout error. fixed #50
  * make sure root meta update after hbase clusters restart. fixed #49

0.3.2 / 2014-02-14
==================

  * org.apache.hadoop.hbase.NotServingRegionException also retry

0.3.1 / 2014-02-14
==================

  * ignore org.apache.hadoop.hbase.NotServingRegionException on split demo
  * action max retry 3 times
  * Action retry with useCache = false when regionserver.WrongRegionException fixed #47
  * add AUTHORS

0.3.0 / 2013-09-25
==================

  * support ping fixed #41

0.2.2 / 2013-09-11
==================

  * remove bagpipe and fix tests

0.2.1 / 2013-09-11
==================

  * TypeError: Cannot set property decodeStrings of undefined isaacs/readable-stream#66

0.2.0 / 2013-09-11
==================

  * fix readFields data empty problems. And also remove mget bigpipe limit #39
  * support coveralls

0.1.11 / 2013-06-20
==================

  * mget: use bagpipe for order
  * add support hbase version
  * rm sending log

0.1.10 / 2013-06-05
==================

  * support Connection timeout on connecting state.

0.1.9 / 2013-06-04
==================

  * add more error info for debug
  * change windows \r\n to \n
  * NoSuchColumnFamilyException should not remove cached region locations. fixed #27

0.1.8 / 2013-05-31
==================

  * add rpcTimeout (@coolme200)

0.1.7 / 2013-05-30
==================

  * support client.getRow(table, row, "*", callback)

0.1.6 / 2013-05-23
==================

  * fix mget: remove default kv.getValue().toString()

0.1.5 / 2013-05-22
==================

  * add getRow select all columns support
  * threshold set to 85
  * add test config
  * fixed #23 when logger not exists, use console default.
  * copyright
  * benchmark support args

0.1.4 / 2013-05-16
==================

  * add mput & mdel fixed #8
  * RegionServerStoppedException need to close connection. fixed #21; mget benchmark.

0.1.3 / 2013-05-16
==================

  * support mget

0.1.2 / 2013-05-14
==================

  * support delete

0.1.1 / 2013-05-14
==================

  * `NotServingRegionException` when querying, need to close the connection. fixed #17
  * long running for get benchmark
  * use blanket instead of jscover

0.1.0 / 2013-05-09
==================

  * remove unused codes
  * clean cache regions when region server socket close. fixed #16 #10
  * remove zkjs deps

0.0.7 / 2013-05-07
==================

  * prefetch table all regions the first time; refetch regions when offline happen. #10 #16
  * add communication protocol, diagram between client and server. fixed #14

0.0.6 / 2013-05-05
==================

  * add benchmark results to readme
  * fixed QPS caculate wrong
  * show use time
  * add getRow and get() benchmark
  * add benchmark.js fixed #7
  * fixed #13 Maximum call stack size exceeded
  * #7 add benchmark
  * fixed #12 , use node-zookeeper-client instead of zkjs.
  * Bytes.toLong move to WritableUtils.toLong

0.0.5 / 2013-05-04
==================

  * prefetchRegionCache() after getClosestRowBefore(). fixed #9 Scan meta, get a table region
  * locateRegion all side region row with startKey #9
  * add scan and scanner #9
  * add scan writable
  * add more debug info

0.0.4 / 2013-05-03
==================

  * support Put fixed #3
  * change prototype code style
  * add coverage, remove htable.js

0.0.3 / 2013-04-28
==================

  * not handle read after every call. fixed #4
  * receiveResponse on nextTick; add more tests for network error cases. fixed #5
  * fix interceptor's error
  * add close, error, request timeout handle #5

0.0.2 / 2013-04-26
==================

  * support `getRow(table, rowkey, columns, callback)`
  * use zookeeper-watcher
  * one hostport one connection

0.0.1 / 2013-04-25
==================

  * support get(table, Get) now.
  * fixed client.prototype
  * client.locateRegion() done
  * remove config.js
  * fixed writeLong 29
  * get region from meta
  * getProtocolVersion() and getClosestRowBefore()
  * writable
  * Result readFields
  * move bytes.js to util/bytes.js; add HbaseObjectWrite
  * rename outStream to DataOutPutBuffer
  * Get readFields done
  * add Get.js
  * dir change
  * add writeChar, writeByte, writeBoolean
  * add writeLong and writeInt
  * update readme
  * add logo
  * Initial commit
