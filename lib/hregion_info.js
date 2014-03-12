/**!
 * node-hbase-client - lib/hregion_info.js
 *
 * Copyright(c) Alibaba Group Holding Limited.
 * MIT Licensed
 *
 * Authors:
 *   苏千 <suqian.yf@alibaba-inc.com> (http://fengmk2.github.com)
 */

'use strict';

/**
 * Module dependencies.
 */

var Long = require('long');
var IOException = require('./errors').IOException;
var md5 = require('utility').md5;
var Bytes = require('./util/bytes');
var HConstants = require('./hconstants');

/**
 * Make a region name of passed parameters.
 *
 * @param tableName
 * @param startKey Can be null
 * @param id Region id (Usually timestamp from when region was created).
 * @param newFormat should we create the region name in the new format
 *                  (such that it contains its encoded name?).
 * @return Region name made of passed tableName, startKey and id
 */
var createRegionName = function (tableName, startKey, id, newFormat) {
  if (!Buffer.isBuffer(tableName)) {
    tableName = Bytes.toBytes(tableName);
  }
  if (id instanceof Long || typeof id === 'number') {
    id = id.toString();
  }
  if (typeof id === 'string') {
    id = Bytes.toBytes(id);
  }
  var length = tableName.length + 2 + id.length + (!startKey ? 0 : startKey.length) +
    (newFormat ? (HRegionInfo.MD5_HEX_LENGTH + 2) : 0);
  var b = new Buffer(length);

  var offset = 0;
  tableName.copy(b, offset);
  offset += tableName.length;
  // System.arraycopy(tableName, 0, b, 0, offset);
  b[offset++] = HRegionInfo.DELIMITER;
  if (startKey && startKey.length > 0) {
    // System.arraycopy(startKey, 0, b, offset, startKey.length);
    startKey.copy(b, offset);
    offset += startKey.length;
  }
  b[offset++] = HRegionInfo.DELIMITER;
  id.copy(b, offset);
  // System.arraycopy(id, 0, b, offset, id.length);
  offset += id.length;

  if (newFormat) {
    //
    // Encoded name should be built into the region name.
    //
    // Use the region name thus far (namely, <tablename>,<startKey>,<id>)
    // to compute a MD5 hash to be used as the encoded name, and append
    // it to the byte buffer.
    //
    // var md5Hash = MD5Hash.getMD5AsHex(b, 0, offset);
    var md5Hash = md5(b.slice(0, offset));
    var md5HashBytes = Bytes.toBytes(md5Hash);

    if (md5HashBytes.length !== HRegionInfo.MD5_HEX_LENGTH) {
      console.error("MD5-hash length mismatch: Expected=" + HRegionInfo.MD5_HEX_LENGTH +
        "; Got=" + md5HashBytes.length);
    }

    // now append the bytes '.<encodedName>.' to the end
    b[offset++] = HRegionInfo.ENC_SEPARATOR;
    md5HashBytes.copy(b, offset);
    // System.arraycopy(md5HashBytes, 0, b, offset, MD5_HEX_LENGTH);
    offset += HRegionInfo.MD5_HEX_LENGTH;
    b[offset++] = HRegionInfo.ENC_SEPARATOR;
  }

  return b;
};

/**
 * HRegion information.
 * Contains HRegion id, start and end keys, a reference to this
 * HRegions' table descriptor, etc.
 */
function HRegionInfo(regionId, tableName, startKey, endKey, split) {
  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableName the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @param regionid Region id to use.
   */
  this.tableName = tableName;

  // This flag is in the parent of a split while the parent is still referenced
  // by daughter regions.  We USED to set this flag when we disabled a table
  // but now table state is kept up in zookeeper as of 0.90.0 HBase.
  this.offLine = false;
  this.regionId = regionId;

  if (!tableName) {
    return;
  }

  var newFormat = true;
  if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME) || Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
    newFormat = false;
  }
  this.regionName = createRegionName(this.tableName, startKey, this.regionId, newFormat);

  this.regionNameStr = Bytes.toStringBinary(this.regionName);
  this.split = split;
  if (endKey === undefined || endKey === null) {
    endKey = HConstants.EMPTY_END_ROW;
  }
  if (startKey === undefined || startKey === null) {
    startKey = HConstants.EMPTY_START_ROW;
  }
  this.endKey = endKey;
  this.startKey = startKey;
}

/**
 * The new format for a region name contains its encodedName at the end.
 * The encoded name also serves as the directory name for the region
 * in the filesystem.
 *
 * New region name format:
 *    &lt;tablename>,,&lt;startkey>,&lt;regionIdTimestamp>.&lt;encodedName>.
 * where,
 *    &lt;encodedName> is a hex version of the MD5 hash of
 *    &lt;tablename>,&lt;startkey>,&lt;regionIdTimestamp>
 *
 * The old region name format:
 *    &lt;tablename>,&lt;startkey>,&lt;regionIdTimestamp>
 * For region names in the old format, the encoded name is a 32-bit
 * JenkinsHash integer value (in its decimal notation, string form).
 *<p>
 * **NOTE**
 *
 * ROOT, the first META region, and regions created by an older
 * version of HBase (0.20 or prior) will continue to use the
 * old region name format.
 */

// VERSION == 0 when HRegionInfo had an HTableDescriptor inside it.
HRegionInfo.VERSION_PRE_092 = 0;
HRegionInfo.VERSION = 1;
/** Separator used to demarcate the encodedName in a region name
 * in the new format. See description on new format above.
 */
HRegionInfo.ENC_SEPARATOR = '.'.charCodeAt(0);
HRegionInfo.MD5_HEX_LENGTH = 32;

/** delimiter used between portions of a region name */
HRegionInfo.DELIMITER = ','.charCodeAt(0);

/**
 * Does region name contain its encoded name?
 *
 * @param regionName region name
 * @return boolean indicating if this a new format region
 *         name which contains its encoded name.
 */
HRegionInfo.hasEncodedName = function (regionName) {
  // check if region name ends in ENC_SEPARATOR
  if ((regionName.length >= 1) && (regionName[regionName.length - 1] === HRegionInfo.ENC_SEPARATOR)) {
    // region name is new format. it contains the encoded name.
    return true;
  }
  return false;
};

/**
 * @param regionName
 * @return the encodedName
 */
HRegionInfo.encodeRegionName = function (regionName) {
  var encodedName;
  if (HRegionInfo.hasEncodedName(regionName)) {
    // region is in new format:
    // <tableName>,<startKey>,<regionIdTimeStamp>/encodedName/
    // encodedName = Bytes.toString(regionName,
    //   regionName.length - HRegionInfo.MD5_HEX_LENGTH - 1,
    //   HRegionInfo.MD5_HEX_LENGTH);
    var offset = regionName.length - HRegionInfo.MD5_HEX_LENGTH - 1;
    encodedName = Bytes.toString(regionName.slice(offset, offset + HRegionInfo.MD5_HEX_LENGTH));
  } else {
    encodedName = Bytes.toString(regionName);
    // old format region name. ROOT and first META region also
    // use this format.EncodedName is the JenkinsHash value.
    // var hashVal = Math.abs(JenkinsHash.getInstance().hash(regionName, regionName.length, 0));
    // encodedName = String.valueOf(hashVal);
  }
  return encodedName;
};

 /**
 * Use logging.
 *
 * @param encodedRegionName The encoded regionname.
 * @return <code>-ROOT-</code> if passed <code>70236052</code> or
 * <code>.META.</code> if passed </code>1028785192</code> else returns
 * <code>encodedRegionName</code>
 */
HRegionInfo.prettyPrint = function (encodedRegionName) {
  if (encodedRegionName.equals("70236052")) {
    return encodedRegionName + "/-ROOT-";
  } else if (encodedRegionName.equals("1028785192")) {
    return encodedRegionName + "/.META.";
  }
  return encodedRegionName;
};

/**
 * Gets the table name from the specified region name.
 *
 * @param regionName
 * @return Table name.
 */
HRegionInfo.getTableName = function (regionName) {
  var offset = -1;
  for (var i = 0; i < regionName.length; i++) {
    if (regionName[i] === HRegionInfo.DELIMITER) {
      offset = i;
      break;
    }
  }
  // var tableName = new byte[offset];
  // System.arraycopy(regionName, 0, tableName, 0, offset);
  return regionName.slice(0, offset);
};

/**
 * Separate elements of a regionName.
 *
 * @param regionName
 * @return Array of byte[] containing tableName, startKey and id
 */
HRegionInfo.parseRegionName = function (regionName) {
  var offset = -1;
  for (var i = 0; i < regionName.length; i++) {
    if (regionName[i] === HRegionInfo.DELIMITER) {
      offset = i;
      break;
    }
  }
  if (offset === -1) {
    throw new IOException("Invalid regionName format");
  }
  // byte[] tableName = new byte[offset];
  // System.arraycopy(regionName, 0, tableName, 0, offset);
  var tableName = regionName.slice(0, offset);
  offset = -1;
  for (i = regionName.length - 1; i > 0; i--) {
    if (regionName[i] === HRegionInfo.DELIMITER) {
      offset = i;
      break;
    }
  }
  if (offset === -1) {
    throw new IOException("Invalid regionName format");
  }
  var startKey = HConstants.EMPTY_BYTE_ARRAY;
  if (offset !== tableName.length + 1) {
    // startKey = new byte[offset - tableName.length - 1];
    // System.arraycopy(regionName, tableName.length + 1, startKey, 0, offset - tableName.length - 1);
    startKey = regionName.slice(tableName.length + 1, offset);
  }
  var idOffset = offset + 1;
  var idLength = regionName.length - offset - 1;
  // var id = new byte[regionName.length - offset - 1];
  // System.arraycopy(regionName, offset + 1, id, 0, regionName.length - offset - 1);
  var id = regionName.slice(idOffset, idOffset + idLength);
  // byte[][] elements = new byte[3][];
  // elements[0] = tableName;
  // elements[1] = startKey;
  // elements[2] = id;
  return [
    tableName,
    startKey,
    id
  ];
};

/**
 * @return the regionName as an array of bytes.
 */
HRegionInfo.prototype.getRegionName = function () {
  return this.regionName;
};

/** @return the startKey */
HRegionInfo.prototype.getStartKey = function () {
  return this.startKey;
};

/** @return the endKey */
HRegionInfo.prototype.getEndKey = function () {
  return this.endKey;
};

/**
 * Get current table name of the region
 *
 * @return byte array of table name
 */
HRegionInfo.prototype.getTableName = function () {
  if (!this.tableName || this.tableName.length === 0) {
    this.tableName = HRegionInfo.getTableName(this.getRegionName());
  }
  return this.tableName;
};

/**
 * Get current table name as string
 *
 * @return string representation of current table
 */
HRegionInfo.prototype.getTableNameAsString = function () {
  return Bytes.toString(this.getTableName());
};

/**
 * Returns true if the given inclusive range of rows is fully contained
 * by this region. For example, if the region is foo,a,g and this is
 * passed ["b","c"] or ["a","c"] it will return true, but if this is passed
 * ["b","z"] it will return false.
 */
HRegionInfo.prototype.containsRange = function (rangeStartKey, rangeEndKey) {
  var firstKeyInRange = Bytes.compareTo(rangeStartKey, this.startKey) >= 0;
  var lastKeyInRange = Bytes.compareTo(rangeEndKey, this.endKey) < 0 ||
    Bytes.equals(this.endKey, HConstants.EMPTY_BYTE_ARRAY);
  return firstKeyInRange && lastKeyInRange;
};

/**
 * Return true if the given row falls in this region.
 */
HRegionInfo.prototype.containsRow = function (row) {
  return Bytes.compareTo(row, this.startKey) >= 0 &&
    (Bytes.compareTo(row, this.endKey) < 0 || Bytes.equals(this.endKey, HConstants.EMPTY_BYTE_ARRAY));
};

/** @return true if this is the root region */
HRegionInfo.prototype.isRootRegion = function () {
  return Bytes.equals(this.tableName, HRegionInfo.ROOT_REGIONINFO.getTableName());
};

/** @return true if this region is from a table that is a meta table,
 * either <code>.META.</code> or <code>-ROOT-</code>
 */
HRegionInfo.prototype.isMetaTable = function () {
  return this.isRootRegion() || this.isMetaRegion();
};

/** @return true if this region is a meta region */
HRegionInfo.prototype.isMetaRegion = function () {
  return Bytes.equals(this.tableName, HRegionInfo.FIRST_META_REGIONINFO.getTableName());
};

/**
 * @return True if has been split and has daughters.
 */
HRegionInfo.prototype.isSplit = function () {
  return this.split;
};

/**
 * @param split set split status
 */
HRegionInfo.prototype.setSplit = function (split) {
  this.split = split;
};

/**
 * @return True if this region is offline.
 */
HRegionInfo.prototype.isOffline = function () {
  return this.offLine;
};

/**
 * The parent of a region split is offline while split daughters hold
 * references to the parent. Offlined regions are closed.
 *
 * @param offLine Set online/offline status.
 */
HRegionInfo.prototype.setOffline = function (offLine) {
  this.offLine = offLine;
};

/**
 * @return True if this is a split parent region.
 */
HRegionInfo.prototype.isSplitParent = function () {
  if (!this.isSplit()) {
    return false;
  }
  if (!this.isOffline()) {
    console.warn("Region is split but NOT offline: " + this);
  }
  return true;
};

/**
 * @see java.lang.Object#toString()
 */
HRegionInfo.prototype.toString = function () {
  return "{ID => " + this.regionId.toString() + ", NAME => '" + this.regionNameStr +
    "', STARTKEY => '" + Bytes.toStringBinary(this.startKey) +
    "', ENDKEY => '" + Bytes.toStringBinary(this.endKey) + "', " +
    // "', ENCODED => " + this.getEncodedName() + "," +
    (this.isOffline() ? " OFFLINE => true," : "") + (this.isSplit() ? " SPLIT => true," : "") + "}";
};

/** @return the object version number */
HRegionInfo.prototype.getVersion = function () {
  return HRegionInfo.VERSION;
};

HRegionInfo.prototype.readFields = function (io) {
  // Read the single version byte.  We don't ask the super class do it
  // because freaks out if its not the current classes' version.  This method
  // can deserialize version 0 and version 1 of HRI.
  var version = io.readByte();
  if (version === 0) {
    // This is the old HRI that carried an HTD.  Migrate it.  The below
    // was copied from the old 0.90 HRI readFields.
    this.endKey = Bytes.readByteArray(io);
    this.offLine = io.readBoolean();
    this.regionId = io.readLong();
    this.regionName = Bytes.readByteArray(io);
    this.regionNameStr = Bytes.toStringBinary(this.regionName);
    this.split = io.readBoolean();
    this.startKey = Bytes.readByteArray(io);
    // try {
    //   HTableDescriptor htd = new HTableDescriptor();
    //   htd.readFields(in);
    //   this.tableName = htd.getName();
    // } catch (EOFException eofe) {
    //   throw new IOException("HTD not found in input buffer", eofe);
    // }
    this.hashCode = io.readInt();
  } else if (this.getVersion() === version) {
    this.endKey = io.readByteArray();
    this.offLine = io.readBoolean();
    this.regionId = io.readLong();
    this.regionName = io.readByteArray();
    this.regionNameStr = Bytes.toStringBinary(this.regionName);
    this.split = io.readBoolean();
    this.startKey = io.readByteArray();
    this.tableName = io.readByteArray();
    this.hashCode = io.readInt();
  } else {
    throw new IOException("Non-migratable/unknown version=" + this.getVersion());
  }
};

HRegionInfo.prototype.compareTo = function (o) {
  if (!o) {
    return 1;
  }

  // Are regions of same table?
  var result = Bytes.compareTo(this.tableName, o.tableName);
  if (result !== 0) {
    return result;
  }

  // Compare start keys.
  result = Bytes.compareTo(this.startKey, o.startKey);
  if (result !== 0) {
    return result;
  }

  // Compare end keys.
  result = Bytes.compareTo(this.endKey, o.endKey);

  if (result !== 0) {
    if (this.startKey.length !== 0 && this.endKey.length === 0) {
      return 1; // this is last region
    }
    if (o.startKey.length !== 0 && o.endKey.length === 0) {
      return -1; // o is the last region
    }
    return result;
  }

  // regionId is usually milli timestamp -- this defines older stamps
  // to be "smaller" than newer stamps in sort order.
  if (this.regionId.greaterThan(o.regionId)) {
    return 1;
  } else if (this.regionId.lessThan(o.regionId)) {
    return -1;
  }

  if (this.offLine === o.offLine) {
    return 0;
  }

  if (this.offLine === true) {
    return -1;
  }

  return 1;
};

HRegionInfo.createRegionName = createRegionName;

/** HRegionInfo for root region */
HRegionInfo.ROOT_REGIONINFO = new HRegionInfo(0, HConstants.ROOT_TABLE_NAME);
HRegionInfo.ROOT_REGIONINFO.isRoot = true;

/** HRegionInfo for first meta region */
HRegionInfo.FIRST_META_REGIONINFO = new HRegionInfo(1, HConstants.META_TABLE_NAME);
HRegionInfo.FIRST_META_REGIONINFO.isMeta = true;


module.exports = HRegionInfo;
