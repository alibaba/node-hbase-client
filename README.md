# hbase-client [![Build Status](https://secure.travis-ci.org/TBEDP/node-hbase-client.png?branch=master)](http://travis-ci.org/TBEDP/node-hbase-client)

![logo](https://raw.github.com/TBEDP/node-hbase-client/master/logo.png)

Asynchronous HBase client for nodejs, pure javascript implementation.

**This project is just developing, Please don't use it on production env.**

* Current State: **Only test on Hbase 0.94**
* [hbase-client](https://github.com/apache/hbase/tree/trunk/hbase-client)

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
client.getRow(table, row, ['f:name', 'f:age'], function (err, r) {
  r.should.have.keys('f:name', 'f:age');
});
```

## TODO

* support `put`
* benchmark
* more stable

## Authors

```bash
$ git summary 

 project  : node-hbase-client
 repo age : 7 weeks
 active   : 11 days
 commits  : 30
 files    : 249
 authors  : 
    29  fengmk2                 96.7%
     1  不四                  3.3%
```

## License

(The MIT License)

Copyright (c) 2013 fengmk2 <fengmk2@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
