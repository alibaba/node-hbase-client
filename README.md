# hbase-client

[![Build Status](https://secure.travis-ci.org/alibaba/node-hbase-client.png?branch=master)](http://travis-ci.org/alibaba/node-hbase-client) [![Coverage Status](https://coveralls.io/repos/alibaba/node-hbase-client/badge.png)](https://coveralls.io/r/alibaba/node-hbase-client)

[![NPM](https://nodei.co/npm/hbase-client.png?downloads=true&stars=true)](https://nodei.co/npm/hbase-client)

![logo](https://raw.github.com/alibaba/node-hbase-client/master/logo.png)

Asynchronous HBase client for Node.js, **pure JavaScript** implementation.

* Current State: Full tests on HBase `0.94.0` and `0.94.16`
* [Java hbase-client](https://github.com/apache/hbase/tree/trunk/hbase-client)

## Support HBase Server Versions

* [√] 0.94.x
    * [√] 0.94.0
    * [√] 0.94.16
* [ ] 0.95.x
* [ ] 0.96.x

## Install

```bash
$ npm install hbase-client
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

### `putRow(table, row, data, callback)`

```js
client.putRow(table, row, {'f1:name': 'foo name', 'f1:age': '18'}, function (err) {
  should.not.exists(err);
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
FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
filterList.addFilter(new FirstKeyOnlyFilter());
filterList.addFilter(new KeyOnlyFilter());
Scan scan = new Scan(Bytes.toBytes("scanner-row0"));
scan.setFilter(filterList);
```

Nodejs code:

```js
var filters = require('hbase').filters;

var filterList = new filters.FilterList(filters.FilterList.Operator.MUST_PASS_ALL);
filterList.addFilter(new filters.FirstKeyOnlyFilter());
filterList.addFilter(new filters.KeyOnlyFilter());
var scan = new Scan('scanner-row0');
scan.setFilter(filterList);

client.getScanner('tcif_acookie_user', scan, function (err, scanner) {\
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

## Benchmarks

@see [docs/benchmark.md](https://github.com/alibaba/node-hbase-client/blob/master/docs/benchmark.md)

## Authors

Thanks for @[haosdent](https://github.com/haosdent) support the test hbase clusters environment and debug helps.

```bash
$ git summary

 project  : node-hbase-client
 repo age : 11 months
 active   : 48 days
 commits  : 129
 files    : 277
 authors  :
   108  fengmk2                 83.7%
    19  tangyao                 14.7%
     1  Alsotang                0.8%
     1  不四                  0.8%
```

## License

(The MIT License)

Copyright (c) 2013 - 2014 Alibaba Group Holding Limited

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
