
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
