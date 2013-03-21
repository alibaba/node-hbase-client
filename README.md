# node-hbase-client [![Build Status](https://secure.travis-ci.org/TBEDP/node-hbase-client.png?branch=master)](http://travis-ci.org/TBEDP/node-hbase-client)

![logo](https://raw.github.com/TBEDP/node-hbase-client/master/logo.png)

Asynchronous HBase client for nodejs.

* [hbase-client](https://github.com/apache/hbase/tree/trunk/hbase-client)

## Java Object Serialize

## HBase 通信过程

### Put 举例

Put rowkey:123 name:test 到 user 表

1. Client 通过 ZooKeeper 获取到 ROOT 表所在的 region server address (IP:Port)
2. ROOT 表只有一行记录，记录着 META 表所在的 region server address
3. 通过读取 META 表确定 User tables 的分区信息
4. 通过 rowkey:123 获取对应的 region server，然后将Put 信息发给它

返回结果：

* 如果正确返回，则代表数据已经写入成功
* 如果错误返回，则根据错误做对应的处理
  * region server 错误 (NotServingRegionException)，则表示 META 已经更新，需要重新获取 META ，重复上述步骤
  * 其他错误，重试
