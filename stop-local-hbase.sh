export HBASE_FILE=`curl http://archive.apache.org/dist/hbase/ | grep -o 'hbase-0.94.27' | head -1`
$HBASE_FILE/bin/stop-hbase.sh
