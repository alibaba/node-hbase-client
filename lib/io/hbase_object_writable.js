/*!
 * node-hbase-client - lib/io/hbase_object_writable.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var debug = require('debug')('hbase:writable');
var Long = require('long');
var Bytes = require('../util/bytes');
var WritableUtils = require('../writable_utils');
var Text = require('../text');
var IOException = require('../errors').IOException;
var UnsupportedOperationException = require('../errors').UnsupportedOperationException;

var CODE_TO_CLASS = {};
var CLASS_TO_CODE = {};

var CLASSES = {};

var addToMap = function (clazzName, code) {
  CLASS_TO_CODE[clazzName] = code;
  CODE_TO_CLASS[code] = clazzName;
};

exports.addToClass = function (name, clazz) {
  CLASSES[name] = clazz;
  clazz.__classname = name;
};

////////////////////////////////////////////////////////////////////////////
// WARNING: Please do not insert, remove or swap any line in this static  //
// block.  Doing so would change or shift all the codes used to serialize //
// objects, which makes backwards compatibility very hard for clients.    //
// New codes should always be added at the end. Code removal is           //
// discouraged because code is a short now.                               //
////////////////////////////////////////////////////////////////////////////

var NOT_ENCODED = 0;
var code = NOT_ENCODED + 1;
// Primitive types.
addToMap('Boolean.TYPE', code++);
addToMap('Byte.TYPE', code++);
addToMap('Character.TYPE', code++);
addToMap('Short.TYPE', code++);
addToMap('Integer.TYPE', code++);
addToMap('Long.TYPE', code++);
addToMap('Float.TYPE', code++);
addToMap('Double.TYPE', code++);
addToMap('Void.TYPE', code++);

// Other java types
addToMap('String.class', code++);
addToMap('byte[].class', code++);
addToMap('byte[][].class', code++);

// Hadoop types
addToMap('Text.class', code++);
addToMap('Writable.class', code++); // 14
addToMap('Writable[].class', code++);
addToMap('HbaseMapWritable.class', code++);
addToMap('NullInstance.class', code++);

// Hbase types
addToMap('HColumnDescriptor.class', code++);
addToMap('HConstants.Modify.class', code++);

addToMap('Integer.class', code++);
addToMap('Integer[].class', code++);

addToMap('MyClass.class', code++);
addToMap('MyClass.class', code++);
addToMap('HRegionInfo.class', code++);
addToMap('HRegionInfo[].class', code++);
addToMap('HServerAddress.class', code++);
addToMap('MyClass.class', code++);
addToMap('HTableDescriptor.class', code++);
addToMap('MyClass.class', code++);

//
// HBASE-880
//
addToMap('MyClass.class', code++);
addToMap('Delete.class', code++);
addToMap('Get.class', code++);
addToMap('KeyValue.class', code++);
addToMap('KeyValue[].class', code++);
addToMap('Put.class', code++);
addToMap('Put[].class', code++);
addToMap('Result.class', code++);
addToMap('Result[].class', code++);
addToMap('Scan.class', code++);

addToMap('WhileMatchFilter.class', code++);
addToMap('PrefixFilter.class', code++);
addToMap('PageFilter.class', code++);
addToMap('InclusiveStopFilter.class', code++);
addToMap('ColumnCountGetFilter.class', code++);
addToMap('SingleColumnValueFilter.class', code++);
addToMap('SingleColumnValueExcludeFilter.class', code++);
addToMap('BinaryComparator.class', code++);
addToMap('BitComparator.class', code++);
addToMap('CompareFilter.class', code++);
addToMap('RowFilter.class', code++);
addToMap('ValueFilter.class', code++);
addToMap('QualifierFilter.class', code++);
addToMap('SkipFilter.class', code++);
addToMap('WritableByteArrayComparable.class', code++);
addToMap('FirstKeyOnlyFilter.class', code++);
addToMap('DependentColumnFilter.class', code++);

addToMap('Delete[].class', code++);

addToMap('MyClass.class', code++);
addToMap('MyClass.class', code++);
addToMap('MyClass.class', code++);

addToMap('List.class', code++);

addToMap('NavigableSet.class', code++);
addToMap('ColumnPrefixFilter.class', code++);

// Multi
addToMap('Row.class', code++);
addToMap('Action.class', code++);
addToMap('MultiAction.class', code++);
addToMap('MultiResponse.class', code++);

// coprocessor execution
addToMap('Exec.class', code++);
addToMap('Increment.class', code++);

addToMap('KeyOnlyFilter.class', code++);

// serializable
addToMap('Serializable.class', code++);

addToMap('RandomRowFilter.class', code++);

addToMap('CompareOp.class', code++);

addToMap('ColumnRangeFilter.class', code++);

addToMap('HServerLoad.class', code++);

addToMap('MyClass.class', code++);

addToMap('HTableDescriptor[].class', code++);

addToMap('Append.class', code++);

addToMap('RowMutations.class', code++);

addToMap('MyClass.class', code++);

//java.lang.reflect.Array is a placeholder for arrays not defined above
exports.GENERIC_ARRAY_CODE = code++;
addToMap('Array.class', exports.GENERIC_ARRAY_CODE);

// make sure that this is the last statement in this static block
exports.NEXT_CLASS_CODE = code;

/**
 * Read a {@link Writable}, {@link String}, primitive type, or an array of
 * the preceding.
 *
 * @param io, input stream.
 * @param objectWritable
 */
exports.readObject = function (io, objectWritable, conf) {
  var code = io.readVInt();
  var declaredClass = CODE_TO_CLASS[code];
  debug('readObject: code: %s, class: %s', code, declaredClass);
  var instance;
  // primitive types
  if (declaredClass === 'Boolean.TYPE') { // boolean
    instance = io.readBoolean();
  } else if (declaredClass === 'Character.TYPE') { // char
    instance = io.readChar();
  } else if (declaredClass === 'Byte.TYPE') { // byte
    instance = io.readByte();
  } else if (declaredClass === 'Short.TYPE') { // short
    instance = io.readShort();
  } else if (declaredClass === 'Integer.TYPE') { // int
    instance = io.readInt();
  } else if (declaredClass === 'Long.TYPE') { // long
    instance = io.readLong();
  } else if (declaredClass === 'Float.TYPE') { // float
    instance = io.readFloat();
  } else if (declaredClass === 'Double.TYPE') { // double
    instance = io.readDouble();
  } else if (declaredClass === 'Void.TYPE') { // void
    instance = null;
    // array
  } else if (declaredClass === 'byte[].class') {
    instance = Bytes.readByteArray(io);
  } else if (declaredClass === 'Result[].class') {
    var Result = CLASSES['Result.class'];
    instance = Result.readArray(io);
  // } else {
  //   var length = io.readInt();
  //   instance = Array.newInstance(declaredClass.getComponentType(), length);
  //   for (var i = 0; i < length; i++) {
  //     Array.set(instance, i, readObject(io, null, conf));
  //   }
  // } else if (declaredClass === 'Array.class') { //an array not declared in CLASS_TO_CODE
  //   // Class<?> componentType = readClass(conf, in);
  //   var length = io.readInt();
    // console.log(declaredClass, length)
    // instance = Array.newInstance(componentType, length);
    // for (int i = 0; i < length; i++) {
    //   Array.set(instance, i, readObject(in, conf));
    // }
  // } else if (List.class.isAssignableFrom(declaredClass)) { // List
  //   int length = in.readInt();
  //   instance = new ArrayList(length);
  //   for (int i = 0; i < length; i++) {
  //     ((ArrayList) instance).add(readObject(in, conf));
  //   }
  } else if (declaredClass === 'String.class') { // String
    // instance = Text.readString(io);
    instance = io.readVString();
  // } else if (declaredClass.isEnum()) { // enum
  //   instance = Enum.valueOf((Class<? extends Enum>) declaredClass, Text.readString(in));

  //   //    } else if (declaredClass == Message.class) {
  //   //      String className = Text.readString(in);
  //   //      try {
  //   //        declaredClass = getClassByName(conf, className);
  //   //        instance = tryInstantiateProtobuf(declaredClass, in);
  //   //      } catch (ClassNotFoundException e) {
  //   //        LOG.error("Can't find class " + className, e);
  //   //        throw new IOException("Can't find class " + className, e);
  //   //      }
  } else {
    // Writable or Serializable
    // int b = (byte) WritableUtils.readVInt(in);
    var b = io.readVInt();
    var name = CODE_TO_CLASS[b];

    debug('writable class: code: %s, name: %s', b, name);

    if (b === NOT_ENCODED) {
      // String className = Text.readString(in);
      name = io.readVString();
      // try {
      //   instanceClass = getClassByName(conf, className);
      // } catch (ClassNotFoundException e) {
      //   LOG.error("Can't find class " + className, e);
      //   throw new IOException("Can't find class " + className, e);
      // }
    } else if (name === 'NullInstance.class') {
      instance = null;
    } else {
      var instanceClass = CLASSES[name];
      instance = instanceClass();

      if (typeof instance.readFields === 'function') {
        instance.readFields(io);
      } else {
        var len = io.readInt();
        var objectBytes = io.read(len);
        instance = objectBytes;
      }
    }

    declaredClass = name;
  }

  if (objectWritable) { // store values
    objectWritable.declaredClass = declaredClass;
    objectWritable.instance = instance;
  }
  return instance;
};

exports.readFields = function (io) {
  var obj = {};
  exports.readObject(io, obj);
  return obj;
};

/**
 * Write a {@link Writable}, {@link String}, primitive type, or an array of
 * the preceding.
 *
 * @param out, output stream.
 * @param instance
 * @param declaredClass
 */
exports.writeObject = function (out, instance, declaredClass) {
  var name;
  var clazz;
  if (!instance && declaredClass === 'Writable.class') {
    instance = new NullInstance(declaredClass);
    clazz = 'NullInstance.class';
  } else {
    name = instance.constructor.name;
    clazz = name + '.class';
  }

  if (Buffer.isBuffer(instance)) {
    clazz = 'byte[].class';
    exports.writeClassCode(out, clazz);
    Bytes.writeByteArray(out, instance);
    return;
  }
  if (instance instanceof Long) {
    clazz = 'Long.TYPE';
    exports.writeClassCode(out, clazz);
    out.writeLong(instance);
    return;
  }

  if (clazz === 'Number.class') {
    // TODO: double, float
    clazz = 'Integer.TYPE';
  }

  if (declaredClass === 'Writable.class') {
    exports.writeClassCode(out, declaredClass);
  } else {
    exports.writeClassCode(out, clazz);
  }

  // writable
  if (typeof instance.write === 'function') {
    if (clazz in CLASS_TO_CODE) {
      exports.writeClassCode(out, clazz);
    } else {
      out.writeByte(NOT_ENCODED);
      Text.writeString(out, instance.constructor.classname);
    }
    instance.write(out);
    return;
  }

  if (clazz === 'String.class') {
    Text.writeString(out, instance);
    return;
  }
  // int
  if (clazz === 'Integer.TYPE') {
    out.writeInt(instance);
    return;
  }

  throw new IOException("Can't write: " + instance + " as " + clazz);
};

/**
 * Write out the code for passed Class.
 *
 * @param out
 * @param c
 */
exports.writeClassCode = function (out, c) {
  var code = CLASS_TO_CODE[c];
  debug('writeClassCode: code: %s, class: %s', code, c);
  if (code === null || code === undefined) {
    throw new UnsupportedOperationException("No code for unexpected " + c);
  }
  WritableUtils.writeVInt(out, code);
};


function NullInstance(declaredClass) {
  this.declaredClass = declaredClass;
}

NullInstance.prototype.write = function (out) {
  exports.writeClassCode(out, this.declaredClass);
};
