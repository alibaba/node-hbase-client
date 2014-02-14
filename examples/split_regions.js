/**!
 * node-hbase-client - examples/split_regions.js
 *
 * Copyright(c) 2014 Alibaba Group Holding Limited.
 *
 * Authors:
 *   苏千 <suqian.yf@taobao.com> (http://fengmk2.github.com)
 */

"use strict";

/**
 * Module dependencies.
 */

var should = require('should');
var hbase = require('../');
var config = require('../test/config');

var client = hbase.Client.create(config);

var rows = [
  "3ed712621c999c5a6d19c8d786f8ce82",
  "2a0752e54fd74ef7b463c0e46886f210",
  "0338472dd25d0faeacbef9b957950961",
  "ed90e50c341eaf887daee18e103bb28b",
  "10d00252d22795c288722e63bc1ca33d",
  "1f032a14c8d8f0b6dd83384ca8c816ef",
  "2ea5a6ca32a713c5769859e3089fec5b",
  "4beacd1fa078854d2059e6516193e7e7",
  "31e43a74ecc31959378cbaa7f159551a",
  "316123fb465a5f8be82533f87f3165a4",
  "de3f37c58f7518700bd407c27ab4bf17",
  "588dd641649da6236866fd9f0818ce05",
  "f029d08c432326120dbe20351d7092c9",
  "e055bc54e5bbc9aabcf31ceba00dcb0a",
  "4ed4ac807ea068dc3fffb33fddaa6de2",
  "5335ef4bbe39a729e58efc83b7978f7a",
  "58ed38f5061b08b2b074539198b20d49",
  "24ecd895c5051f767d85744bd823b9ae",
  "fb53fc27f3b09e1a5dd8d22940eb449f",
  "c03ab1046b08a7e85b3e5c2129769197",
  "09fcf7f30947c0aab91fa710b896a78f",
  "ee3585dce03fd5f0e536f74ef02d2bb5",
  "ae03246e28eb66a34ba8040ec321f9f8",
  "cca185cc33799c7eaee6f5133c295fd4",
  "4eef6c87fa0683a9f57982871449db9d",
  "04050fafefed971a05dc5b70e88d622f",
  "622178712478f57679b5e57ee836673c",
  "694fa55d0b2cdc1344b1bea24640c476",
  "c6fe17f283680a62793da3ef55704c15",
];

var index = 0;

function get() {
  var row = rows[index++];
  if (index >= rows.length) {
    index = 0;
  }
  client.getRow('bulkwriter_test', row, ['cf:w02'], function (err, result) {
    if (err) {
      if (err.name === 'org.apache.hadoop.hbase.NotServingRegionException') {
        console.log(index - 1, err.name);
        return;
      }
      throw err;
    }
    should.exist(result);
    result.should.have.property('cf:w02');
    console.log('%s: %s', index - 1, row);
  });
}

setInterval(get, 1000);
