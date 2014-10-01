// hbase org.apache.hadoop.hbase.util.RegionSplitter hbase_client_test_table HexStringSplit -c 5 -f cf1
// hbase org.apache.hadoop.hbase.util.RegionSplitter hbase_client_test_table_actions HexStringSplit -c 5 -f cf1

var config = {
  // 0.94.x
  zookeeperHosts: [
    'localhost'
  ],
  zookeeperRoot: '/hbase',

  // logger: console,
  logger: {
    warn: function () {},
    info: function () {},
    error: function () {},
  },
  rpcTimeout: 40000,
  hostnamePart: 'local',
  invalidHost: 'localhost',
  invalidPort: 65535,
  tableUser: 'hbase_client_test_table',
  tableActions: 'hbase_client_test_table_actions',
  regionServer: 'localhost.localdomain:60020',
};

config.clusters = [
  {
    zookeeperHosts: config.zookeeperHosts,
    zookeeperRoot: config.zookeeperRoot,
  },
];

module.exports = config;
