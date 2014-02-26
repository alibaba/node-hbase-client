# serialization

## Get.java

```
| 1byte |            |
|version| bytes size |
| 1 - 2 | int        |
```

## [WritableUtils.java](http://hadoop.apache.org/docs/current/api/src-html/org/apache/hadoop/io/WritableUtils.html)

http://hadoop.apache.org/docs/current/api/src-html/org/apache/hadoop/io/WritableUtils.html#line.378

```java
/**
374       * Parse the first byte of a vint/vlong to determine the number of bytes
375       * @param value the first byte of the vint/vlong
376       * @return the total number of bytes (1 to 9)
377       */
378      public static int decodeVIntSize(byte value) {
379        if (value >= -112) {
380          return 1;
381        } else if (value < -120) {
382          return -119 - value;
383        }
384        return -111 - value;
385      }
```

## Java Object Serialize

## HBase 通信过程

### Put 举例

Put rowkey:123 name:test 到 user 表

1. Client 通过 ZooKeeper 获取到 ROOT 表所在的 region server address (IP:Port)
2. ROOT 表只有一行记录，记录着 META 表所在的 region server address
3. 通过读取 META 表确定 User tables 的分区信息
4. 通过 rowkey:123 获取对应的 region server，然后将Put 信息发给它

返回结果：

* 如果正确返回，则代表数据已经写入成功
* 如果错误返回，则根据错误做对应的处理
  * region server 错误 (NotServingRegionException)，则表示 META 已经更新，需要重新获取 META ，重复上述步骤
  * 其他错误，重试

## Java环境

* [mac 之配置java src.jar文件](http://xiuqiuka-hotmail-com.iteye.com/blog/1128242)
