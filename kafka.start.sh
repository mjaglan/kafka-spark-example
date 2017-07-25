# KAFKA HOME DIRECTORY
cd $KAFKA_HOME

# KAFKA START IN BACKGROUND
nohup ./bin/kafka-server-start.sh ./config/server.properties > ./kafka.log 2>&1 &

# KAFKA PRODUCER
# echo "Hello, World" | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic2 > /dev/null

# KAFKA CONSUMER
# ./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic topic2,topic1 --from-beginning

# KAFKA TOPICS LIST
./bin/kafka-topics.sh --list --zookeeper localhost:2181

# KAFKA DELETE A TOPIC
# ./bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic mytopic

# KAFKA CONSUMER OFFSET FOR A TOPIC
# ./bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --broker-info --group test_group --topic test_topic --zookeeper localhost:2181

# KAFKA CLUSTER LOGS
tail -f kafka.log

