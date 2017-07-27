# should have logs directory
LOGS_DIR="./logs"
mkdir -p $LOGS_DIR

# name a logs file
TYPE="produce"
CURRENT_TIME=$(date "+%Y.%m.%d-%H.%M.%S")
LOGS_FILE=$LOGS_DIR"/launch-spark-"$TYPE"-"$CURRENT_TIME".log"

# # Run application locally # #
# http://spark.apache.org/docs/latest/submitting-applications.html
$SPARK_HOME/bin/spark-submit --class edu.iu.soic.cs.entryPointJava --master local[*]                   ./build/libs/kafka.spark-all.jar $TYPE > $LOGS_FILE
# https://spark.apache.org/docs/1.6.3/running-on-yarn.html
# $SPARK_HOME/bin/spark-submit --class edu.iu.soic.cs.entryPointJava --master yarn --deploy-mode client  ./build/libs/kafka.spark-all.jar $TYPE > $LOGS_FILE
# $SPARK_HOME/bin/spark-submit --class edu.iu.soic.cs.entryPointJava --master yarn --deploy-mode cluster ./build/libs/kafka.spark-all.jar $TYPE > $LOGS_FILE
