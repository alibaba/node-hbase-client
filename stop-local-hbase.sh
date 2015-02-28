export HBASE_FILE=`curl http://www.eu.apache.org/dist/hbase/ | grep -o 'hbase-0.94.[0-9]*' | head -1`
$HBASE_FILE/bin/stop-hbase.sh
