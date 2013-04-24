var ZK = require('zkjs')

// onfiguration.set("hbase.zookeeper.quorum",
//                 "10.232.98.74,10.232.98.75,10.232.98.76,10.232.98.77,10.232.98.78");
//           configuration.set("hbase.zookeeper.property.clientPort",
//                 "2181");
//           configuration.set("zookeeper.znode.parent",
//                 "/hbase-rd-test-0.94");

var zk = new ZK({
    hosts: ['10.232.98.74:2181', '10.232.98.75:2182', '10.232.98.76:2183', 
        '10.232.98.77:2183', '10.232.98.78:2183'],
    root: '/hbase-rd-test-0.94'
});

zk.start(function (err) {

    zk.getChildren(
        '/',
        function (err, children, zstat) {
            console.log(arguments)
            if (!err) {
                console.log('/', 'has', children.length, 'children')
            }
        }
    )

    zk.get(
        '/root-region-server',
        function (watch) {
            console.log(watch)
            console.log(watch.path, 'was', watch.type)
        },
        function (err, value, zstat) {
            console.log(arguments)
            console.log('root-region-server value is ', value.toString())
        }
    )

    zk.get(
        '/master',
        function (watch) {
            console.log(watch)
            console.log(watch.path, 'was', watch.type)
        },
        function (err, value, zstat) {
            console.log(arguments)
            console.log('master value is ', value.toString())
        }
    )
});
