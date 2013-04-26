var ZK = require('zkjs');
var EventEmitter = require('events').EventEmitter;

// onfiguration.set("hbase.zookeeper.quorum",
//                 "10.232.98.74,10.232.98.75,10.232.98.76,10.232.98.77,10.232.98.78");
//           configuration.set("hbase.zookeeper.property.clientPort",
//                 "2181");
//           configuration.set("zookeeper.znode.parent",
//                 "/hbase-rd-test-0.94");

var zk = new ZK({
  hosts: ['10.232.36.107:12181'],
  root: '/zkjs-test',
  maxReconnectAttempts: 1024,
  logger: console,
  // hosts: ['10.232.98.74:2181', '10.232.98.75:2182', '10.232.98.76:2183', 
  //     '10.232.98.77:2183', '10.232.98.78:2183'],
  // root: '/hbase-rd-test-0.94'
});

zk.on('expired', function () {
  // clean up and reconnect
  console.log('crap, my ephemeral nodes are gone');
  zk.start();
});

zk.on('started', function (err) {

  zk.getChildren(
    '/',
    function (err, children, zstat) {
      console.log(arguments);
      if (!err) {
        console.log('/', 'has', children.length, 'children');
      }
    }
  );

  var ev = new EventEmitter();
  ev.start = function () {
    zk.get('/root', function (watch) {
      ev.start();
    }, function (err, value, zstat) {
      // console.log(arguments)
      ev.emit('value', value);
    });
  };

  ev.on('value', function (value) {
    console.log('value: %s', value.toString());
  });
  // ev.start();

  // zk.on('changed', function (path) {
  //   console.log('the node at', path, 'changed');
  // });

  
  var version = 1573;
  setInterval(function () {
    var value = 'root ' + Date.now();
    zk.set('/root', value, version++, function () {
      // console.log('set root %s %j', value, arguments);
    });

    // zk.create('/root4', 'this is root2', function (errno, path) {
    //   console.log(arguments);
    // });
  }, 600000);

  // zk.mkdirp('/root', function (err) {
  //   console.log(arguments);
  // });

  // zk.get(
  //   '/root-region-server',
  //   function (watch) {
  //     console.log(watch)
  //     console.log(watch.path, 'was', watch.type)
  //   },
  //   function (err, value, zstat) {
  //     console.log(arguments)
  //     console.log('root-region-server value is ', value.toString())
  //   }
  // );

  // zk.get(
  //   '/master',
  //   function (watch) {
  //     console.log(watch)
  //     console.log(watch.path, 'was', watch.type)
  //   },
  //   function (err, value, zstat) {
  //     console.log(arguments)
  //     console.log('master value is ', value.toString())
  //   }
  // );
});

zk.start();

