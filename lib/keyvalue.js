/*!
 * node-hbase-client - lib/keyvalue.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Bytes = require('./util/bytes');
var WritableUtils = require('./writable_utils');
var HConstants = require('./hconstants');

/**
 * Key type.
 * Has space for other key types to be added later.  Cannot rely on
 * enum ordinals . They change if item is removed or moved.  Do our own codes.
 */
var Type = {
  Minimum: 0,
  Put: 4,
  Delete: 8, 
  DeleteColumn: 12, 
  DeleteFamily: 14,
  // Maximum is used when searching; you look from maximum on down.
  Maximum: 255,
};

/**
 * An HBase Key/Value.  This is the fundamental HBase Type.
 *
 * <p>If being used client-side, the primary methods to access individual fields
 * are {@link #getRow()}, {@link #getFamily()}, {@link #getQualifier()},
 * {@link #getTimestamp()}, and {@link #getValue()}.  These methods allocate new
 * byte arrays and return copies. Avoid their use server-side.
 *
 * <p>Instances of this class are immutable.  They do not implement Comparable
 * but Comparators are provided.  Comparators change with context,
 * whether user table or a catalog table comparison.  Its critical you use the
 * appropriate comparator.  There are Comparators for KeyValue instances and
 * then for just the Key portion of a KeyValue used mostly by {@link HFile}.
 *
 * <p>KeyValue wraps a byte array and takes offsets and lengths into passed
 * array at where to start interpreting the content as KeyValue.  The KeyValue
 * format inside a byte array is:
 * <keylength> <valuelength> <key> <value>
 * Key is further decomposed as:
 * <rowlength> <row> <columnfamilylength> <columnfamily> <columnqualifier> <timestamp> <keytype>
 * The `rowlength` maximum is `Short.MAX_SIZE`,
 * column family length maximum is
 * `Byte.MAX_SIZE`, and column qualifier + key length must
 * be < `Integer.MAX_SIZE`.
 * The column does not contain the family/qualifier delimiter, {@link #COLUMN_FAMILY_DELIMITER}
 *
 * KeyValue format:
 * | 4 bytes   | 4 bytes     |     |       |
 * | keylength | valuelength | key | value |
 *
 * Key format:
 * | 2 bytes   |     | 1 byte             |              |                        |  8 bytes  | 1 byte  |
 * | rowlength | row | columnfamilylength | columnfamily | columnfamily qualifier | timestamp | keytype |
 */
function KeyValue(bytes, offset, length) {
  this.bytes = bytes;
  this.offset = offset || 0;
  this.length = length || bytes.length;
  this.keyLength = 0;
  // default value is 0, aka DNC
  this.memstoreTS = 0;
  this.rowCache = null;
  this.timestampCache = -1;
}

/**
 * Colon character in UTF-8
 */
KeyValue.COLUMN_FAMILY_DELIMITER = ':';
KeyValue.COLUMN_FAMILY_DELIM_ARRAY = new Buffer(KeyValue.COLUMN_FAMILY_DELIMITER);

/** Size of the key length field in bytes*/
KeyValue.KEY_LENGTH_SIZE = Bytes.SIZEOF_INT;

/** Size of the key type field in bytes */
KeyValue.TYPE_SIZE = Bytes.SIZEOF_BYTE;

/** Size of the row length field in bytes */
KeyValue.ROW_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

/** Size of the family length field in bytes */
KeyValue.FAMILY_LENGTH_SIZE = Bytes.SIZEOF_BYTE;

/** Size of the timestamp field in bytes */
KeyValue.TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;

// Size of the timestamp and type byte on end of a key -- a long + a byte.
KeyValue.TIMESTAMP_TYPE_SIZE = KeyValue.TIMESTAMP_SIZE + KeyValue.TYPE_SIZE;

// Size of the length shorts and bytes in key.
KeyValue.KEY_INFRASTRUCTURE_SIZE = KeyValue.ROW_LENGTH_SIZE + 
  KeyValue.FAMILY_LENGTH_SIZE + KeyValue.TIMESTAMP_TYPE_SIZE;

// How far into the key the row starts at. First thing to read is the short
// that says how long the row is.
KeyValue.ROW_OFFSET = Bytes.SIZEOF_INT /*keylength*/+ Bytes.SIZEOF_INT /*valuelength*/;

// Size of the length ints in a KeyValue datastructure.
KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE = KeyValue.ROW_OFFSET;

KeyValue.humanReadableTimestamp = function (timestamp) {
  if (timestamp === HConstants.LATEST_TIMESTAMP) {
    return "LATEST_TIMESTAMP";
  }
  if (timestamp === HConstants.OLDEST_TIMESTAMP) {
    return "OLDEST_TIMESTAMP";
  }
  return String.valueOf(timestamp);
};

//---------------------------------------------------------------------------
//
//  String representation
//
//---------------------------------------------------------------------------

KeyValue.prototype.toString = function () {
  if (!this.bytes || this.bytes.length === 0) {
    return "empty";
  }
  var family = this.getFamily();
  if (family) {
    family = family.toString();
  }
  var qualifier = this.getQualifier();
  if (qualifier) {
    qualifier = qualifier.toString();
  }
  var timestamp = this.getTimestamp().toString();
  return this.getRow().toString() + '/' + family + '/' + qualifier + '/' + timestamp + 
    "/vlen=" + this.getValueLength() + "/ts=" + this.memstoreTS;
};

//---------------------------------------------------------------------------
//
//  Public Member Accessors
//
//---------------------------------------------------------------------------

/**
 * @return The byte array backing this KeyValue.
 */
KeyValue.prototype.getBuffer = function () {
  return this.bytes;
};

/**
 * @return Offset into {@link #getBuffer()} at which this KeyValue starts.
 */
KeyValue.prototype.getOffset = function () {
  return this.offset;
};

/**
 * @return Length of bytes this KeyValue occupies in {@link #getBuffer()}.
 */
KeyValue.prototype.getLength = function () {
  return this.length;
};

//---------------------------------------------------------------------------
//
//  Length and Offset Calculators
//
//---------------------------------------------------------------------------

/**
 * @return Key offset in backing buffer..
 */
KeyValue.prototype.getKeyOffset = function () {
  return this.offset + KeyValue.ROW_OFFSET;
};

KeyValue.prototype.getKeyString = function () {
  return Bytes.toStringBinary(this.getBuffer(), this.getKeyOffset(), this.getKeyLength());
};

/**
 * @return Length of key portion.
 */
KeyValue.prototype.getKeyLength = function () {
  if (this.keyLength === 0) {
    this.keyLength = Bytes.toInt(this.bytes, this.offset);
  }
  return this.keyLength;
};

/**
 * @return Value offset
 */
KeyValue.prototype.getValueOffset = function () {
  return this.getKeyOffset() + this.getKeyLength();
};

/**
 * @return Value length
 */
KeyValue.prototype.getValueLength = function () {
  return Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
};

/**
 * @return Row offset
 */
KeyValue.prototype.getRowOffset = function () {
  return this.getKeyOffset() + Bytes.SIZEOF_SHORT;
};

/**
 * @return Row length
 */
KeyValue.prototype.getRowLength = function () {
  return Bytes.toShort(this.bytes, this.getKeyOffset());
};

/**
 * @return Family offset
 */
KeyValue.prototype.getFamilyOffset = function (rlength) {
  rlength = rlength || this.getRowLength();
  // row offset + rowlength data(2 bytes) + rowlength + familylength data(1 byte)
  return this.offset + KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT + rlength + Bytes.SIZEOF_BYTE;
};

/**
 * @return Family length
 */
KeyValue.prototype.getFamilyLength = function (familyOffset) {
  familyOffset = familyOffset || this.getFamilyOffset();
  return this.bytes[familyOffset - 1];
};

/**
 * @return Qualifier offset
 */
KeyValue.prototype.getQualifierOffset = function (familyOffset) {
  familyOffset = familyOffset || this.getFamilyOffset();
  return familyOffset + this.getFamilyLength(familyOffset);
};

/**
 * @return Qualifier length
 */
KeyValue.prototype.getQualifierLength = function (rlength, flength) {
  rlength = rlength || this.getRowLength();
  flength = flength || this.getFamilyLength();
  return this.getKeyLength() - (KeyValue.KEY_INFRASTRUCTURE_SIZE + rlength + flength);
};

/**
 * @return Column (family + qualifier) length
 */
KeyValue.prototype.getTotalColumnLength = function (rlength, foffset) {
  rlength = rlength || this.getRowLength();
  foffset = foffset || this.getFamilyOffset(rlength);
  var flength = this.getFamilyLength(foffset);
  var qlength = this.getQualifierLength(rlength, flength);
  return flength + qlength;
};

/**
 * @param keylength Pass if you have it to save on a int creation.
 * @return Timestamp offset
 */
KeyValue.prototype.getTimestampOffset = function (keylength) {
  keylength = keylength || this.getKeyLength();
  return this.getKeyOffset() + keylength - KeyValue.TIMESTAMP_TYPE_SIZE;
};

/**
 * @return True if this KeyValue has a LATEST_TIMESTAMP timestamp.
 */
KeyValue.prototype.isLatestTimestamp = function () {
  return Bytes.equals(this.getBuffer(), this.getTimestampOffset(), Bytes.SIZEOF_LONG, 
    HConstants.LATEST_TIMESTAMP_BYTES, 0, Bytes.SIZEOF_LONG);
};

/**
 * Do not use unless you have to.  Used internally for compacting and testing.
 *
 * Use {@link #getRow()}, {@link #getFamily()}, {@link #getQualifier()}, and
 * {@link #getValue()} if accessing a KeyValue client-side.
 * @return Copy of the key portion only.
 */
KeyValue.prototype.getKey = function () {
  var keyLength = this.getKeyLength();
  var offset = this.getKeyOffset();
  return this.bytes.slice(offset, offset + keyLength);
};

/**
 * Returns value in a new byte array.
 * Primarily for use client-side. If server-side, use
 * {@link #getBuffer()} with appropriate offsets and lengths instead to
 * save on allocations.
 * @return Value in a new byte array.
 */
KeyValue.prototype.getValue = function () {
  var o = this.getValueOffset();
  var l = this.getValueLength();
  return this.getBuffer().slice(o, o + l);
};

/**
 * Primarily for use client-side.  Returns the row of this KeyValue in a new
 * byte array.<p>
 *
 * If server-side, use {@link #getBuffer()} with appropriate offsets and
 * lengths instead.
 * @return Row in a new byte array.
 */
KeyValue.prototype.getRow = function () {
  if (this.rowCache === null) {
    var o = this.getRowOffset();
    var l = this.getRowLength();
    // initialize and copy the data into a local variable
    // in case multiple threads race here.
    this.rowCache = this.getBuffer().slice(o, o + l);
  }
  return this.rowCache;
};

KeyValue.prototype.getTimestamp = function () {
  if (this.timestampCache === -1) {
    var tsOffset = this.getTimestampOffset();
    this.timestampCache = WritableUtils.toLong(this.bytes.slice(tsOffset, tsOffset + Bytes.SIZEOF_LONG));
  }
  return this.timestampCache;
};

/**
 * @param keylength Pass if you have it to save on a int creation.
 * @return Type of this KeyValue.
 */
KeyValue.prototype.getType = function (keylength) {
  keylength = keylength || this.getKeyLength();
  return this.bytes[this.offset + keylength - 1 + KeyValue.ROW_OFFSET];
};

/**
 * Primarily for use client-side.  Returns the family of this KeyValue in a
 * new byte array.<p>
 *
 * If server-side, use {@link #getBuffer()} with appropriate offsets and
 * lengths instead.
 * @return Returns family. Makes a copy.
 */
KeyValue.prototype.getFamily = function () {
  var o = this.getFamilyOffset();
  var l = this.getFamilyLength(o);
  return this.bytes.slice(o, o + l);
};

/**
 * Primarily for use client-side.  Returns the column qualifier of this
 * KeyValue in a new byte array.<p>
 *
 * If server-side, use {@link #getBuffer()} with appropriate offsets and
 * lengths instead.
 * Use {@link #getBuffer()} with appropriate offsets and lengths instead.
 * @return Returns qualifier. Makes a copy.
 */
KeyValue.prototype.getQualifier = function () {
  var o = this.getQualifierOffset();
  var l = this.getQualifierLength();
  return this.bytes.slice(o, o + l);
};

KeyValue.Type = Type;

KeyValue.createKeyValue = function (row, family, qualifier, timestamp, type, value) {
  var rlength = row.length;
  // Family length
  var flength = family ? family.length : 0;
  // Qualifier length
  var qlength = qualifier ? qualifier.length : 0;
  // Key length
  var keylength = KeyValue.KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
  // Value length
  var vlength = value ? value.length : 0;
  // Allocate right-sized byte array.
  var bytes = new Buffer(KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength);
  // Write key, value and key row length.
  var pos = 0;
  pos = Bytes.putInt(bytes, pos, keylength);
  pos = Bytes.putInt(bytes, pos, vlength);
  pos = Bytes.putShort(bytes, pos, rlength);
  pos = Bytes.putBytes(bytes, pos, row);
  pos = Bytes.putByte(bytes, pos, flength);
  if (flength !== 0) {
    pos = Bytes.putBytes(bytes, pos, family);
  }
  if (qlength !== 0) {
    pos = Bytes.putBytes(bytes, pos, qualifier);
  }
  pos = Bytes.putLong(bytes, pos, timestamp);
  pos = Bytes.putByte(bytes, pos, type);
  if (value && value.length > 0) {
    pos = Bytes.putBytes(bytes, pos, value);
  }

  return new KeyValue(bytes);
};

KeyValue.prototype.write = function (out) {
  out.writeInt(this.getLength());
  out.write(this.getBuffer(), this.getOffset(), this.getLength());
};


module.exports = KeyValue;
