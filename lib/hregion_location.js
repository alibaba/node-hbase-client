/*!
 * node-hbase-client - lib/hregion_location.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

/**
 * Data structure to hold HRegionInfo and the address for the hosting
 * HRegionServer.  Immutable.  Comparable, but we compare the 'location' only:
 * i.e. the hostname and port, and *not* the regioninfo.  This means two
 * instances are the same if they refer to the same 'location' (the same
 * hostname and port), though they may be carrying different regions.
 */
function HRegionLocation(regionInfo, hostname, port) {
  // Cache of the 'toString' result.
  this.cachedString = null;
  // Cache of the hostname + port
  this.cachedHostnamePort = null;

  /**
   * Constructor
   * @param regionInfo the HRegionInfo for the region
   * @param hostname Hostname
   * @param port port
   */
  // public HRegionLocation(HRegionInfo regionInfo, final String hostname, final int port) {
  this.regionInfo = regionInfo;
  this.hostname = hostname;
  this.port = port;
}

HRegionLocation.prototype = {
  toString: function () {
    if (this.cachedString === null) {
      this.cachedString = "region=" + this.regionInfo.toString() + 
        ", hostname=" + this.hostname + ", port=" + this.port;
    }
    return this.cachedString;
  },

  /** @return HRegionInfo */
  getRegionInfo: function () {
    return this.regionInfo;
  },

  getServerAddress: function () {
    return {hostname: this.hostname, port: this.port};
  },

  getHostname: function () {
    return this.hostname;
  },

  getPort: function () {
    return this.port;
  },

  getHostnamePort: function () {
    if (this.cachedHostnamePort === null) {
      this.cachedHostnamePort = this.hostname + ':' + this.port;
    }
    return this.cachedHostnamePort;
  },
};


module.exports = HRegionLocation;
