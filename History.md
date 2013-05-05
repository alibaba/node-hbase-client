
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
