# hbase-client

[![NPM version][npm-image]][npm-url]
[![build status][travis-image]][travis-url]
[![Test coverage][coveralls-image]][coveralls-url]
[![Gittip][gittip-image]][gittip-url]
[![David deps][david-image]][david-url]
[![node version][node-image]][node-url]
[![npm download][download-image]][download-url]

[npm-image]: https://img.shields.io/npm/v/hbase-client.svg?style=flat-square
[npm-url]: https://npmjs.org/package/hbase-client
[travis-image]: https://img.shields.io/travis/alibaba/node-hbase-client.svg?style=flat-square
[travis-url]: https://travis-ci.org/alibaba/node-hbase-client
[coveralls-image]: https://img.shields.io/coveralls/alibaba/node-hbase-client.svg?style=flat-square
[coveralls-url]: https://coveralls.io/r/alibaba/node-hbase-client?branch=master
[gittip-image]: https://img.shields.io/gittip/fengmk2.svg?style=flat-square
[gittip-url]: https://www.gittip.com/fengmk2/
[david-image]: https://img.shields.io/david/alibaba/node-hbase-client.svg?style=flat-square
[david-url]: https://david-dm.org/alibaba/node-hbase-client
[node-image]: https://img.shields.io/badge/node.js-%3E=_0.10-green.svg?style=flat-square
[node-url]: http://nodejs.org/download/
[download-image]: https://img.shields.io/npm/dm/hbase-client.svg?style=flat-square
[download-url]: https://npmjs.org/package/hbase-client

![logo](https://raw.github.com/alibaba/node-hbase-client/master/logo.png)

Asynchronous HBase client for Node.js, **pure JavaScript** implementation.

* Current State: Full tests on HBase `0.94.0` and `0.94.16`
* [Java hbase-client](https://github.com/apache/hbase/tree/trunk/hbase-client)

## Support HBase Server Versions

* [√] 0.94.x
    * [√] 0.94.0
    * [√] 0.94.16
* [ ] 0.96.x
* [ ] 0.98.x

If you're use HBase >= 0.96.x,
please use [hbase-rpc-client](https://www.npmjs.com/package/hbase-rpc-client) which CoffeeScript HBase Implementation with protobuf.

## Install

```bash
$ npm install hbase-client --save
```

## Run Unit Test

Start local hbase server

```bash
$ ./start-local-hbase.sh
```

If everything go fine, run tests

```bash
$ make test
```

Stop hbase server

```bash
$ ./stop-local-hbase.sh
```

## Get Started with `CRUD`

* Create a hbase client through zookeeper:

```js
var HBase = require('hbase-client');

var client = HBase.create({
  zookeeperHosts: [
    '127.0.0.1:2181' // only local zookeeper
  ],
  zookeeperRoot: '/hbase-0.94.16',
});
```

* Put a data row to hbase:

```js
client.putRow('someTableName', 'rowkey1', {'f1:name': 'foo name', 'f1:age': '18'}, function (err) {
  console.log(err);
});
```

* Get the row we put:

```js
client.getRow('someTableName', 'rowkey1', ['f1:name', 'f1:age'], function (err, row) {
  console.log(row);
});
```

* Delete the row we put:

```js
client.deleteRow('someTableName', 'rowkey1', function (err) {
  console.log(err);
});
```

## Usage

### `get(table, get, callback)`: Get a row from a table

```js
var HBase = require('hbase-client');

var client = HBase.create({
  zookeeperHosts: [
    '127.0.0.1:2181', '127.0.0.1:2182',
  ],
  zookeeperRoot: '/hbase-0.94',
});

// Get `f1:name, f2:age` from `user` table.
var param = new HBase.Get('foo');
param.addColumn('f1', 'name');
param.addColumn('f1', 'age');

client.get('user', param, function (err, result) {
  console.log(err);
  var kvs = result.raw();
  for (var i = 0; i < kvs.length; i++) {
    var kv = kvs[i];
    console.log('key: `%s`, value: `%s`', kv.toString(), kv.getValue().toString());
  }
});
```

### `getRow(table, rowkey, columns, callback)`

```js
client.getRow(table, row, ['f:name', 'f:age'], function (err, row) {
  row.should.have.keys('f:name', 'f:age');
});

// return all columns, like `select *`
client.getRow(table, row, function (err, row) {
  row.should.have.keys('f:name', 'f:age', 'f:gender');
});

client.getRow(table, row, '*', function (err, row) {
  row.should.have.keys('f:name', 'f:age', 'f:gender');
});
```

### `put(table, put, callback)`: Put a row to table

```js
var put = new HBase.Put('foo');
put.add('f1', 'name', 'foo');
put.add('f1', 'age', '18');
client.put('user', put, function (err) {
  console.log(err);
});
```

### `putRow(table, rowKey, data, callback)`

```js
client.putRow(table, rowKey, {'f1:name': 'foo name', 'f1:age': '18'}, function (err) {
  should.not.exists(err);
  client.getRow(table, rowKey, function (err, row) {
    should.not.exist(err);
    should.exist(row);
    // {
    //   'cf1:age': <Buffer 31 38>,
    //   'cf1:name': <Buffer 66 6f 6f 20 6e 61 6d 65>
    // }
    row['cf1:name'].toString().should.equal('foo name');
    row['cf1:age'].toString().should.equal('18');
    done();
  });
});
```

### `delete(tableName, del, callback)`

```js
var del = new Delete(rowkey);
del.deleteColumns('f', 'name-t');
client.delete(table, del, function (err, result) {
  //TODO:...
});
```

```js
var del = new Delete(rowkey);
del.deleteFamily('f');
client.delete(table, del, function (err, result) {
  //TODO:...
});
```

### `deleteRow(tableName, rowkey, callback)`

```js
var tableName = 'user_search';
var rowkey = 'rowkeyyyyyy';
client.deleteRow(tableName, rowkey, function (err) {
  //TODO:...
});
```

### `mget(tableName, rows, columns, callback)`

```js
var rows = ['row1', 'row2'];
var columns = ['f:col1', 'f:col2'];
client.mget(tableName, rows, columns, function (err, results){
  //TODO:...
});
```

### `mput(tableName, rows, callback)`

```js
var rows = [{row: 'rowkey1', 'f:col1': 'col_value'}, {row: 'rowkey2', 'f:col1': 'col_value'}];
client.mput(tableName, rows, function (err, results) {
  //TODO:...
});
```

### `mdelete(tableName, rowkeys, callback)`

```js
var rowKeys = ['rowkey1', 'rowkey2'];
client.mdelete(tableName, rowKeys, function (err, results) {
  //TODO:...
});

```

## Scan

### Scan table and return row key only

Java code:

```java
FilterList filterList = new FilterList({operator: FilterList.Operator.MUST_PASS_ALL});
filterList.addFilter(new FirstKeyOnlyFilter());
filterList.addFilter(new KeyOnlyFilter());
Scan scan = new Scan(Bytes.toBytes("scanner-row0"));
scan.setFilter(filterList);
```

Nodejs code:

```js
var filters = require('hbase-client').filters;

var filterList = new filters.FilterList({operator: filters.FilterList.Operator.MUST_PASS_ALL});
filterList.addFilter(new filters.FirstKeyOnlyFilter());
filterList.addFilter(new filters.KeyOnlyFilter());
var scan = new Scan('scanner-row0');
scan.setFilter(filterList);

client.getScanner('user', scan, function (err, scanner) {\
  var index = 0;
  var next = function (numberOfRows) {
    scanner.next(numberOfRows, function (err, rows) {
      // console.log(rows)
      should.not.exists(err);
      if (rows.length === 0) {
        index.should.equal(5);
        return scanner.close(done);
      }

      rows.should.length(1);

      var closed = false;
      rows.forEach(function (row) {
        var kvs = row.raw();
        var r = {};
        for (var i = 0; i < kvs.length; i++) {
          var kv = kvs[i];
          kv.getRow().toString().should.equal('scanner-row' + index++);
          kv.toString().should.include('/vlen=0/');
          console.log(kv.getRow().toString(), kv.toString())
        }
      });

      if (closed) {
        return scanner.close(done);
      }

      next(numberOfRows);
    });
  };

  next(1);
});
```

### Scan table and return row filtered by single column value

Java code:

```java
byte [] family = Bytes.toBytes("cf1");
byte [] qualifier = Bytes.toBytes("qualifier2");
FilterList filterList = new FilterList({operator: FilterList.Operator.MUST_PASS_ALL});
filterList.addFilter(new SingleColumnValueFilter(family, qualifier, CompareOp.LESS_OR_EQUAL, Bytes.toBytes("scanner-row0 cf1:qualifier2")));
filterList.addFilter(new SingleColumnValueFilter(family, qualifier, CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("scanner-"))));
filterList.addFilter(new SingleColumnValueFilter(family, qualifier, CompareOp.NOT_EQUAL, new BitComparator(Bytes.toBytes("0"), BitComparator.BitwiseOp.XOR)));
filterList.addFilter(new SingleColumnValueFilter(family, qualifier, CompareOp.NOT_EQUAL, new NullComparator()));
filterList.addFilter(new SingleColumnValueFilter(family, qualifier, CompareOp.EQUAL, new RegexStringComparator("scanner-*"))));
filterList.addFilter(new SingleColumnValueFilter(family, qualifier, CompareOp.EQUAL, new SubstringComparator("cf1:qualifier2"))));
Scan scan = new Scan(Bytes.toBytes("scanner-row0"));
scan.setFilter(filterList);
```

Nodejs code:

```js
var filterList = new filters.FilterList({operator: filters.FilterList.Operator.MUST_PASS_ALL});
var family = 'cf1';
var qualifier = 'qualifier2';
filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'LESS_OR_EQUAL', 'scanner-row0 cf1:qualifier2'));
filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'GREATER_OR_EQUAL', new filters.BinaryPrefixComparator('scanner-')));
filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'NOT_EQUAL', new filters.BitComparator('0', filters.BitComparator.BitwiseOp.XOR)));
filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'NOT_EQUAL', new filters.NullComparator()));
filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'EQUAL', new filters.RegexStringComparator('scanner-*')));
filterList.addFilter(new filters.SingleColumnValueFilter(family, qualifier, 'EQUAL', new filters.SubstringComparator('cf1:qualifier2')));
var scan = new Scan('scanner-row0');
scan.setFilter(filterList);

client.getScanner('user', scan, function (err, scanner) {\
  //TODO:...
});
```

## TODO

- [√] support `put`
- [√] benchmark
- [√] more stable
- [√] support `delete`
- [√] multi actions
    - [√] multi get
    - [√] multi put
    - [√] multi delete
- [√] fail retry
- [ ] filters
    - [√] FilterList
    - [√] FirstKeyOnlyFilter
    - [√] KeyOnlyFilter
    - [√] SingleColumnValueFilter

## Benchmarks

@see [docs/benchmark.md](https://github.com/alibaba/node-hbase-client/blob/master/docs/benchmark.md)

## Authors

Thanks for @[haosdent](https://github.com/haosdent) support the test hbase clusters environment and debug helps.

```bash
$ git summary

 project  : node-hbase-client
 repo age : 1 year, 7 months
 active   : 70 days
 commits  : 195
 files    : 297
 authors  :
   155	fengmk2                 79.5%
    20	tangyao                 10.3%
    15	Martin Cizek            7.7%
     1	coolme200               0.5%
     1	Alsotang                0.5%
     1	不四                  0.5%
     1	Lukas Benes             0.5%
     1	Vaclav Loffelmann       0.5%
```

## License

(The MIT License)

Copyright (c) 2013 - 2014 Alibaba Group Holding Limited

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
