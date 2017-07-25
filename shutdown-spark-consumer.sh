ps -ef | grep spark |  grep "kafka.spark-all.jar consume" | awk '{print $2}'   | xargs kill

HDFS_OUT_DIR="/user/ddos"

LOCAL_OUT_DIR="./resources/output"

hdfs dfs -ls $HDFS_OUT_DIR

mkdir -p $LOCAL_OUT_DIR

rm -rf $LOCAL_OUT_DIR/*

hadoop fs -copyToLocal $HDFS_OUT_DIR $LOCAL_OUT_DIR/


