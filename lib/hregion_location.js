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

  /**
   * @see java.lang.Object#toString()
   */
  toString: function () {
    if (this.cachedString === null) {
      this.cachedString = "region=" + this.regionInfo.getRegionNameAsString() + 
        ", hostname=" + this.hostname + ", port=" + this.port;
    }
    return this.cachedString;
  },

  // /**
  //  * @see java.lang.Object#equals(java.lang.Object)
  //  */
  // equals(Object o) {
  //   if (this == o) {
  //     return true;
  //   }
  //   if (o == null) {
  //     return false;
  //   }
  //   if (!(o instanceof HRegionLocation)) {
  //     return false;
  //   }
  //   return this.compareTo((HRegionLocation) o) == 0;
  // }

  // /**
  //  * @see java.lang.Object#hashCode()
  //  */
  // @Override
  // public int hashCode() {
  //   int result = this.hostname.hashCode();
  //   result ^= this.port;
  //   return result;
  // }

  /** @return HRegionInfo */
  getRegionInfo: function () {
    return this.regionInfo;
  },

  /**
   * Do not use!!! Creates a HServerAddress instance which will do a resolve.
   * @return HServerAddress
   * @deprecated Use {@link #getHostnamePort}
   */
  getServerAddress: function () {
    return {hostname: this.hostname, port: this.port};
  },

  getHostname: function () {
    return this.hostname;
  },

  getPort: function () {
    return this.port;
  },

  /**
   * @return String made of hostname and port formatted as per {@link Addressing#createHostAndPortStr(String, int)}
   */
  getHostnamePort: function () {
    if (this.cachedHostnamePort === null) {
      this.cachedHostnamePort = this.hostname + ':' + this.port;
    }
    return this.cachedHostnamePort;
  },

  //
  // Comparable
  //

  // public int compareTo(HRegionLocation o) {
  //   int result = this.hostname.compareTo(o.getHostname());
  //   if (result != 0)
  //     return result;
  //   return this.port - o.getPort();
  // }
};


module.exports = HRegionLocation;
