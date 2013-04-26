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
  this.offset = offset;
  this.length = length;
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

/**
 * Comparator for plain key/values; i.e. non-catalog table key/values.
 */
// KeyValue.COMPARATOR = new KVComparator();

/**
 * Comparator for plain key; i.e. non-catalog table key.  Works on Key portion
 * of KeyValue only.
 */
// KeyValue.KEY_COMPARATOR = new KeyComparator();

/**
 * A {@link KVComparator} for <code>.META.</code> catalog table
 * {@link KeyValue}s.
 */
// KeyValue.META_COMPARATOR = new MetaComparator();

/**
 * A {@link KVComparator} for <code>.META.</code> catalog table
 * {@link KeyValue} keys.
 */
// KeyValue.META_KEY_COMPARATOR = new MetaKeyComparator();

/**
 * A {@link KVComparator} for <code>-ROOT-</code> catalog table
 * {@link KeyValue}s.
 */
// KeyValue.ROOT_COMPARATOR = new RootComparator();

/**
 * A {@link KVComparator} for <code>-ROOT-</code> catalog table
 * {@link KeyValue} keys.
 */
// KeyValue.ROOT_KEY_COMPARATOR = new RootKeyComparator();

/**
 * Get the appropriate row comparator for the specified table.
 *
 * Hopefully we can get rid of this, I added this here because it's replacing
 * something in HSK.  We should move completely off of that.
 *
 * @param tableName  The table name.
 * @return The comparator.
 */
KeyValue.getRowComparator = function (tableName) {
  if (Bytes.equals(HTableDescriptor.ROOT_TABLEDESC.getName(), tableName)) {
    return ROOT_COMPARATOR.getRawComparator();
  }
  if (Bytes.equals(HTableDescriptor.META_TABLEDESC.getName(), tableName)) {
    return META_COMPARATOR.getRawComparator();
  }
  return COMPARATOR.getRawComparator();
};

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

/**
 * Create a KeyValue for the specified row, family and qualifier that would be
 * smaller than all other possible KeyValues that have the same row,family,qualifier.
 * Used for seeking.
 * @param row - row key (arbitrary byte array)
 * @param family - family name
 * @param qualifier - column qualifier
 * @return First possible key on passed <code>row</code>, and column.
 */
// KeyValue.createFirstOnRow = function (row, family, qualifier) {
//   var bytes = createByteArray(row, roffset, rlength, family, foffset, flength, qualifier, qoffset, qlength, timestamp,
//     type, value, voffset, vlength);
//   return new KeyValue(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Maximum);
// };

KeyValue.prototype = {
  //---------------------------------------------------------------------------
  //
  //  String representation
  //
  //---------------------------------------------------------------------------

  toString: function () {
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
  },

  //---------------------------------------------------------------------------
  //
  //  Public Member Accessors
  //
  //---------------------------------------------------------------------------

  /**
   * @return The byte array backing this KeyValue.
   */
  getBuffer: function () {
    return this.bytes;
  },

  /**
   * @return Offset into {@link #getBuffer()} at which this KeyValue starts.
   */
  getOffset: function () {
    return this.offset;
  },

  /**
   * @return Length of bytes this KeyValue occupies in {@link #getBuffer()}.
   */
  getLength: function () {
    return this.length;
  },

  //---------------------------------------------------------------------------
  //
  //  Length and Offset Calculators
  //
  //---------------------------------------------------------------------------

  /**
   * Determines the total length of the KeyValue stored in the specified
   * byte array and offset.  Includes all headers.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @return length of entire KeyValue, in bytes
   */
  // private static int getLength(byte[] bytes, int offset) {
  //   return ROW_OFFSET + Bytes.toInt(bytes, offset) + Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);
  // }

  /**
   * @return Key offset in backing buffer..
   */
  getKeyOffset: function () {
    return this.offset + KeyValue.ROW_OFFSET;
  },

  getKeyString: function () {
    return Bytes.toStringBinary(this.getBuffer(), this.getKeyOffset(), this.getKeyLength());
  },

  /**
   * @return Length of key portion.
   */
  getKeyLength: function () {
    if (this.keyLength === 0) {
      this.keyLength = Bytes.toInt(this.bytes, this.offset);
    }
    return this.keyLength;
  },

  /**
   * @return Value offset
   */
  getValueOffset: function () {
    return this.getKeyOffset() + this.getKeyLength();
  },

  /**
   * @return Value length
   */
  getValueLength: function () {
    return Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
  },

  /**
   * @return Row offset
   */
  getRowOffset: function () {
    return this.getKeyOffset() + Bytes.SIZEOF_SHORT;
  },

  /**
   * @return Row length
   */
  getRowLength: function () {
    return Bytes.toShort(this.bytes, this.getKeyOffset());
  },

  /**
   * @return Family offset
   */
  getFamilyOffset: function (rlength) {
    rlength = rlength || this.getRowLength();
    // row offset + rowlength data(2 bytes) + rowlength + familylength data(1 byte)
    return this.offset + KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT + rlength + Bytes.SIZEOF_BYTE;
  },

  /**
   * @return Family length
   */
  getFamilyLength: function (familyOffset) {
    familyOffset = familyOffset || this.getFamilyOffset();
    return this.bytes[familyOffset - 1];
  },

  /**
   * @return Qualifier offset
   */
  getQualifierOffset: function (familyOffset) {
    familyOffset = familyOffset || this.getFamilyOffset();
    return familyOffset + this.getFamilyLength(familyOffset);
  },

  /**
   * @return Qualifier length
   */
  getQualifierLength: function (rlength, flength) {
    rlength = rlength || this.getRowLength();
    flength = flength || this.getFamilyLength();
    return this.getKeyLength() - (KeyValue.KEY_INFRASTRUCTURE_SIZE + rlength + flength);
  },

  /**
   * @return Column (family + qualifier) length
   */
  getTotalColumnLength: function (rlength, foffset) {
    rlength = rlength || this.getRowLength();
    foffset = foffset || this.getFamilyOffset(rlength);
    var flength = this.getFamilyLength(foffset);
    var qlength = this.getQualifierLength(rlength, flength);
    return flength + qlength;
  },

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Timestamp offset
   */
  getTimestampOffset: function (keylength) {
    keylength = keylength || this.getKeyLength();
    return this.getKeyOffset() + keylength - KeyValue.TIMESTAMP_TYPE_SIZE;
  },

  /**
   * @return True if this KeyValue has a LATEST_TIMESTAMP timestamp.
   */
  isLatestTimestamp: function () {
    return Bytes.equals(this.getBuffer(), this.getTimestampOffset(), Bytes.SIZEOF_LONG, 
      HConstants.LATEST_TIMESTAMP_BYTES, 0, Bytes.SIZEOF_LONG);
  },

  /**
   * @param now Time to set into <code>this</code> IFF timestamp ==
   * {@link HConstants#LATEST_TIMESTAMP} (else, its a noop).
   * @return True is we modified this.
   */
  // updateLatestStamp: function (now) {
  //   if (this.isLatestTimestamp()) {
  //     var tsOffset = this.getTimestampOffset();
  //     System.arraycopy(now, 0, this.bytes, tsOffset, Bytes.SIZEOF_LONG);
  //     return true;
  //   }
  //   return false;
  // },

  /**
   * Do not use unless you have to.  Used internally for compacting and testing.
   *
   * Use {@link #getRow()}, {@link #getFamily()}, {@link #getQualifier()}, and
   * {@link #getValue()} if accessing a KeyValue client-side.
   * @return Copy of the key portion only.
   */
  getKey: function () {
    var keyLength = this.getKeyLength();
    var offset = this.getKeyOffset();
    return this.bytes.slice(offset, offset + keyLength);
  },

  /**
   * Returns value in a new byte array.
   * Primarily for use client-side. If server-side, use
   * {@link #getBuffer()} with appropriate offsets and lengths instead to
   * save on allocations.
   * @return Value in a new byte array.
   */
  getValue: function () {
    var o = this.getValueOffset();
    var l = this.getValueLength();
    return this.getBuffer().slice(o, o + l);
  },

  /**
   * Primarily for use client-side.  Returns the row of this KeyValue in a new
   * byte array.<p>
   *
   * If server-side, use {@link #getBuffer()} with appropriate offsets and
   * lengths instead.
   * @return Row in a new byte array.
   */
  getRow: function () {
    if (this.rowCache === null) {
      var o = this.getRowOffset();
      var l = this.getRowLength();
      // initialize and copy the data into a local variable
      // in case multiple threads race here.
      this.rowCache = this.getBuffer().slice(o, o + l);
    }
    return this.rowCache;
  },

  /**
   * @return Timestamp
   */
  getTimestamp: function () {
    if (this.timestampCache === -1) {
      var tsOffset = this.getTimestampOffset();
      this.timestampCache = Bytes.toLong(this.bytes.slice(tsOffset, tsOffset + Bytes.SIZEOF_LONG));
    }
    return this.timestampCache;
  },

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Type of this KeyValue.
   */
  getType: function (keylength) {
    keylength = keylength || this.getKeyLength();
    return this.bytes[this.offset + keylength - 1 + KeyValue.ROW_OFFSET];
  },

  /**
   * Primarily for use client-side.  Returns the family of this KeyValue in a
   * new byte array.<p>
   *
   * If server-side, use {@link #getBuffer()} with appropriate offsets and
   * lengths instead.
   * @return Returns family. Makes a copy.
   */
  getFamily: function () {
    var o = this.getFamilyOffset();
    var l = this.getFamilyLength(o);
    return this.bytes.slice(o, o + l);
  },

  /**
   * Primarily for use client-side.  Returns the column qualifier of this
   * KeyValue in a new byte array.<p>
   *
   * If server-side, use {@link #getBuffer()} with appropriate offsets and
   * lengths instead.
   * Use {@link #getBuffer()} with appropriate offsets and lengths instead.
   * @return Returns qualifier. Makes a copy.
   */
  getQualifier: function () {
    var o = this.getQualifierOffset();
    var l = this.getQualifierLength();
    return this.bytes.slice(o, o + l);
  },

};


module.exports = KeyValue;
