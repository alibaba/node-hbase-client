'use strict';

var Readable = require('readable-stream').Readable;

var fs = require('fs');

var s = fs.createReadStream(__filename);

var readable = new Readable();
readable.wrap(s);

console.log(readable.read(1));

readable.on('readable', function () {
  console.log(readable.read(20).toString());
});
