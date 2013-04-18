var fs = require('fs');

var bf = fs.readFileSync('/tmp/hbase.bytes');

console.log(bf)
for (var i = 0; i < bf.length; i++) {
  console.log(bf[i]);
}