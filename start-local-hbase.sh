export HBASE_FILE=`curl http://archive.apache.org/dist/hbase/ | grep -o 'hbase-0.94.27' | head -1`
if [ ! -d $HBASE_FILE ]; then
  curl http://archive.apache.org/dist/hbase/$HBASE_FILE/$HBASE_FILE.tar.gz -o $HBASE_FILE.tar.gz
  tar -xzf $HBASE_FILE.tar.gz
fi
cp test/conf/hbase-site.xml $HBASE_FILE/conf/
cp test/bin/regionservers.sh $HBASE_FILE/bin/
cp test/bin/zookeepers.sh $HBASE_FILE/bin/
$HBASE_FILE/bin/start-hbase.sh
$HBASE_FILE/bin/hbase org.apache.hadoop.hbase.util.RegionSplitter hbase_client_test_table HexStringSplit -c 10 -f cf1
$HBASE_FILE/bin/hbase org.apache.hadoop.hbase.util.RegionSplitter hbase_client_test_table_actions HexStringSplit -c 10 -f cf1
echo "disable 'hbase_client_test_table';\
  alter 'hbase_client_test_table', {NAME=>'cf1', VERSIONS => 2};\
  enable 'hbase_client_test_table';\
  disable 'hbase_client_test_table_actions';\
  alter 'hbase_client_test_table_actions', {NAME=>'cf1', VERSIONS => 5};\
  enable 'hbase_client_test_table_actions'" | JRUBY_OPTS= $HBASE_FILE/bin/hbase shell
