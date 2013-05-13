/*!
 * hbase-client - test/hregion_info.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var should = require('should');
var HRegionInfo = require('../lib/hregion_info');
var utils = require('./support/utils');
var Bytes = require('../lib/util/bytes');


describe('hregion_info.test.js', function () {
  
  var testJavaBytes = utils.createTestBytes('hregion_info');

  describe('createRegionName()', function () {
    it('should write region name to bytes', function () {
      var rootname = HRegionInfo.createRegionName(Bytes.toBytes("-ROOT-"), null, 0, false);
      testJavaBytes('createRegionName', '-ROOT-,,0', rootname);
      rootname.length.should.equal(HRegionInfo.ROOT_REGIONINFO.regionName.length);
    });
  });

});
