/*!
 * node-hbase-client - lib/io/hbase_object_writable.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Bytes = require('../util/bytes');

var CODE_TO_CLASS = {};
var CLASS_TO_CODE = {};

var CLASSES = {
};

var addToMap = function (clazz, code) {
  // CLASS_TO_CODE[clazz] = code;
  CODE_TO_CLASS[code] = clazz;
};

exports.addToClass = function (name, clazz) {
  CLASSES[name] = clazz;
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
addToMap('Writable.class', code++);
addToMap('Writable[].class', code++);
addToMap('HbaseMapWritable.class', code++);
addToMap('NullInstance.class', code++);

// Hbase types
addToMap('HColumnDescriptor.class', code++);
addToMap('HConstants.Modify.class', code++);

// We used to have a class named HMsg but its been removed.  Rather than
// just axe it, use following random Integer class -- we just chose any
// class from java.lang -- instead just so codes that follow stay
// in same relative place.
addToMap('Integer.class', code++);
addToMap('Integer[].class', code++);

addToMap('MyClass.class', code++);//addToMap(HRegion.class, code++);
addToMap('MyClass.class', code++);//addToMap(HRegion[].class, code++);
addToMap('HRegionInfo.class', code++);
addToMap('HRegionInfo[].class', code++);
addToMap('HServerAddress.class', code++);
addToMap('MyClass.class', code++);//addToMap(HServerInfo.class, code++);
addToMap('HTableDescriptor.class', code++);
addToMap('MyClass.class', code++);//addToMap(MapWritable.class, code++);

//
// HBASE-880
//
addToMap('MyClass.class', code++);//addToMap(ClusterStatus.class, code++);
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

addToMap('MyClass.class', code++);//addToMap(HLog.Entry.class, code++);
addToMap('MyClass.class', code++);//addToMap(HLog.Entry[].class, code++);
addToMap('MyClass.class', code++);//addToMap(HLogKey.class, code++);

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

addToMap('MyClass.class', code++);//addToMap(RegionOpeningState.class, code++);

addToMap('HTableDescriptor[].class', code++);

addToMap('Append.class', code++);

addToMap('RowMutations.class', code++);

addToMap('MyClass.class', code++);//addToMap(Message.class, code++);

//java.lang.reflect.Array is a placeholder for arrays not defined above
exports.GENERIC_ARRAY_CODE = code++;
addToMap('Array.class', exports.GENERIC_ARRAY_CODE);

// make sure that this is the last statement in this static block
exports.NEXT_CLASS_CODE = code;

/**
 * Read a {@link Writable}, {@link String}, primitive type, or an array of
 * the preceding.
 * @param in
 * @param objectWritable
 * @param conf
 * @return the object
 * @throws IOException
 */
exports.readObject = function (io, objectWritable, conf) {
  var declaredClass = CODE_TO_CLASS[io.readVInt()];
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
  } else if (declaredClass === 'Array.class') { //an array not declared in CLASS_TO_CODE
    // Class<?> componentType = readClass(conf, in);
    var length = io.readInt();
    console.log(declaredClass, length)
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
    var instanceClass = null;
    // int b = (byte) WritableUtils.readVInt(in);
    var b = io.readVInt() & 0xff;
    if (b === NOT_ENCODED) {
      // String className = Text.readString(in);
      var className = io.readVString();
      // try {
      //   instanceClass = getClassByName(conf, className);
      // } catch (ClassNotFoundException e) {
      //   LOG.error("Can't find class " + className, e);
      //   throw new IOException("Can't find class " + className, e);
      // }
    } else {
      instanceClass = CLASSES[CODE_TO_CLASS[b]];
    }
    if (typeof instanceClass.readFields === 'function') {
      instance = instanceClass.readFields(io);
      // if (instanceClass == NullInstance.class) { // null
      //   declaredClass = ((NullInstance) instance).declaredClass;
      //   instance = null;
      // }
    } else {
      var len = io.readInt();
      var objectBytes = io.read(len);
      instance = objectBytes;
      // ByteArrayInputStream bis = null;
      // ObjectInputStream ois = null;
      // try {
      //   bis = new ByteArrayInputStream(objectBytes);
      //   ois = new ObjectInputStream(bis);
      //   instance = ois.readObject();
      // } catch (ClassNotFoundException e) {
      //   LOG.error("Class not found when attempting to deserialize object", e);
      //   throw new IOException("Class not found when attempting to " + "deserialize object", e);
      // } finally {
      //   if (bis != null)
      //     bis.close();
      //   if (ois != null)
      //     ois.close();
      // }
    }
  }
  if (objectWritable) { // store values
    objectWritable.declaredClass = declaredClass;
    objectWritable.instance = instance;
  }
  return instance;
};
