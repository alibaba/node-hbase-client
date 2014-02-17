# Benchmark

Version: `hbase-client@0.1.0`

**Only one node process, one connection for one region(hostname:port).**

System info:

2 CPUs: **Intel(R) Xeon(R) CPU E5520  @ 2.27GHz**

```bash
$ uname -a
Linux v010076.sqa.cm4 2.6.18-274.el5xen #1 SMP Fri Jul 8 17:45:44 EDT 2011 x86_64 x86_64 x86_64 GNU/Linux

$ lsb_release -a
LSB Version:  :core-4.0-amd64:core-4.0-ia32:core-4.0-noarch:graphics-4.0-amd64:graphics-4.0-ia32:graphics-4.0-noarch:printing-4.0-amd64:printing-4.0-ia32:printing-4.0-noarch
Release:  5.7
Codename: Tikanga

$ node -v
v0.8.21
```

### Results: hbase-client@0.1.0

[benchmark.js](https://github.com/TBEDP/node-hbase-client/blob/master/benchmark.js)

```bash
$ node benchmark.js $Concurrency
```

Method | Concurrency | QPS     | RT(ms)
 ---   |  ---        | ------- | ------
Get    | 1           | 1714    | 0.57
Get    | 2           | 3097 ↑  | 0.63
Get    | 5           | 3391 ↑  | 1.46
Get    | 10          | 3649 ↑  | 2.73
Get    | 20          | 3720 ↑  | **5.36**
Get    | 40          | 3732 ↑  | 10.7
Get    | 50          | 3923 ↑  | 12.73
Get    | 100         | 3800 ↓  | 26.28
Get    | 150         | 3772 ↓  | 39.73
Get    | 200         | 3860 ↑  | 51.72
Get    | 500         | 3674 ↓  | 135.45
Get    | 1000        | 3658 ↓  | 270.4
Put    | 1           | 677     | 1.47
Put    | 2           | 1250 ↑  | 1.59
Put    | 5           | 2604 ↑  | 1.91
Put    | 8           | 3134 ↑  | 2.54
Put    | 10          | 3298 ↑  | 3.02
Put    | 15          | 3367 ↑  | 4.44
Put    | 20          | 3484 ↑  | **5.73**
Put    | 30          | 3487 ↑  | 8.59
Put    | 40          | 3416 ↓  | 11.7
Put    | 50          | 3559 ↑  | 14.03
Put    | 100         | 3630 ↑  | 27.53
Put    | 150         | 3510 ↓  | 42.7
Put    | 200         | 3534 ↑  | 56.53
Put    | 500         | 3429 ↓  | 144.93
Put    | 1000        | 3328 ↓  | 297.26

### Get()

```bash
---------------- Get() --------------------
CPU: 27%
MEM: 55m
Concurrency: 1
Total: 58336 ms
QPS: 1714
RT 0.57 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 86%
MEM: 55m
Concurrency: 2
Total: 32293 ms
QPS: 3097
RT 0.63 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 57m
Concurrency: 5
Total: 29491 ms
QPS: 3391
RT 1.46 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 57m
Concurrency: 10
Total: 27407 ms
QPS: 3649
RT 2.73 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 57m
Concurrency: 20
Total: 26881 ms
QPS: 3720
RT 5.36 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 58m
Concurrency: 40
Total: 26795 ms
QPS: 3732
RT 10.7 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 59m
Concurrency: 50
Total: 25493 ms
QPS: 3923
RT 12.73 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 59m
Concurrency: 100
Total: 26313 ms
QPS: 3800
RT 26.28 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 59m
Concurrency: 150
Total: 26514 ms
QPS: 3772
RT 39.73 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 59m
Concurrency: 200
Total: 25910 ms
QPS: 3860
RT 51.72 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 63m
Concurrency: 500
Total: 27218 ms
QPS: 3674
RT 135.45 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Get() --------------------
CPU: 99%
MEM: 68m
Concurrency: 1000
Total: 27335 ms
QPS: 3658
RT 270.4 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------
```

### Put()

```bash
---------------- Put() --------------------
CPU: 7%
MEM: 36m
Concurrency: 1
Total: 59109 ms
QPS: 677
RT 1.47 ms
Total 40000, Success: 40000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 18%
MEM: 54m
Concurrency: 2
Total: 79999 ms
QPS: 1250
RT 1.59 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 72%
MEM: 56m
Concurrency: 5
Total: 49922 ms
QPS: 2604
RT 1.91 ms
Total 130000, Success: 130000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 90%
MEM: 58m
Concurrency: 8
Total: 47865 ms
QPS: 3134
RT 2.54 ms
Total 150000, Success: 150000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 98%
MEM: 57m
Concurrency: 10
Total: 63680 ms
QPS: 3298
RT 3.02 ms
Total 210000, Success: 210000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 99%
MEM: 59m
Concurrency: 15
Total: 35643 ms
QPS: 3367
RT 4.44 ms
Total 120000, Success: 120000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 59m
Concurrency: 20
Total: 40180 ms
QPS: 3484
RT 5.73 ms
Total 140000, Success: 140000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 60m
Concurrency: 30
Total: 37278 ms
QPS: 3487
RT 8.59 ms
Total 130000, Success: 130000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 60m
Concurrency: 40
Total: 29274 ms
QPS: 3416
RT 11.7 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 60m
Concurrency: 50
Total: 36528 ms
QPS: 3559
RT 14.03 ms
Total 130000, Success: 130000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 60m
Concurrency: 100
Total: 55102 ms
QPS: 3630
RT 27.53 ms
Total 200000, Success: 200000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 60m
Concurrency: 150
Total: 28494 ms
QPS: 3510
RT 42.7 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 61m
Concurrency: 200
Total: 28295 ms
QPS: 3534
RT 56.53 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 64m
Concurrency: 500
Total: 29162 ms
QPS: 3429
RT 144.93 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------

---------------- Put() --------------------
CPU: 100%
MEM: 68m
Concurrency: 1000
Total: 30046 ms
QPS: 3328
RT 297.26 ms
Total 100000, Success: 100000, Fail: 0
-------------------------------------------
```
