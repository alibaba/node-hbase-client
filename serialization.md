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

