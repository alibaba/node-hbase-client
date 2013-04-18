/*!
 * node-hbase-client - text/fixtures/show.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var fs = require('fs');
var path = require('path');

var names = fs.readdirSync(__dirname);

names.forEach(function (name) {
  if (name.indexOf('.bytes') < 0) {
    return;
  }
  var p = path.join(__dirname, name);
  var buf = fs.readFileSync(p);
  console.log(name, buf);
});
