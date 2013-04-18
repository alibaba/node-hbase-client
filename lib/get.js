/*!
 * node-hbase-client - lib/get.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

function Get(row) {
  this.row = row;
}

Get.prototype.write = function (out) {
  out.writeByte(GET_VERSION);
  Bytes.writeByteArray(out, this.row);
  out.writeLong(this.lockId);
  out.writeInt(this.maxVersions);
  if (this.filter == null) {
    out.writeBoolean(false);
  } else {
    out.writeBoolean(true);
    Bytes.writeByteArray(out, Bytes.toBytes(filter.getClass().getName()));
    filter.write(out);
  }
  out.writeBoolean(this.cacheBlocks);
  tr.write(out);
  out.writeInt(familyMap.size());
  for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
    Bytes.writeByteArray(out, entry.getKey());
    NavigableSet<byte[]> columnSet = entry.getValue();
    if (columnSet == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(columnSet.size());
      for (byte[] qualifier : columnSet) {
        Bytes.writeByteArray(out, qualifier);
      }
    }
  }
  writeAttributes(out);
};


