package edu.iu.soic.cs.produce

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import java.sql.{Connection, DriverManager, ResultSet}
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import java.util._
import java.util.Calendar
import kafka.server._
import kafka.producer._
import kafka.common._
import kafka._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer._
import scala.collection.JavaConverters._


// Producer
class  ProducerEntryPoint
object ProducerEntryPoint {
   def main(args: Array[String]) {
     
      println("------------------------------- PRODUCER -------------------------------")
     
      if(args.length > 0 && args(0)=="--help") {
          println("arguments[4]: #partitions  topicName  csvInputFile  kafkaBroker")
      }
      
      // Get #partitions  topicName  csvInputFile  kafkaBroker
      val numPartitions = args(0).toInt
      val topicName = args(1)
      val logFileName = args(2)
      val broker = args(3)
            

      val conf = new SparkConf().
      setAppName("producer-spark-kafka-demo-app").
      setMaster("local[*]"). // for local testing
      set("spark.driver.allowMultipleContexts", "false")
      
      val sc  = SparkContext.getOrCreate(conf)     

      // Read txt file, parse it
      val logFileRDD = sc.textFile(logFileName)
      
            
      // ********** Add topic to the pool ****************
      // Create a ZooKeeper client
      val sessionTimeoutMs = 10000
      val connectionTimeoutMs = 10000
      
      // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
      // createTopic() will only seem to work (it will return without error).  The topic will exist in
      // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
      // topic.
      val zkClient = new ZkClient("localhost:2181", sessionTimeoutMs, connectionTimeoutMs,
          ZKStringSerializer)
      
      // Create a topic named "myTopic" with 8 partitions and a replication factor of 3
      val replicationFactor = 1
      val topicConfig = new Properties
      scala.util.control.Exception.ignoring(classOf[TopicExistsException]) {
        AdminUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, topicConfig)
      }


      // Boradcast direct kafka messages with brokers and topics
      val props = new HashMap[String, Object]()
      props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
      props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,      "org.apache.kafka.common.serialization.StringSerializer")
      props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,        "org.apache.kafka.common.serialization.StringSerializer")

      logFileRDD.foreachPartition((aPartition: scala.collection.Iterator[String]) => {
        val producer = new KafkaProducer[String,String](props)
        aPartition.foreach((line: String) => {
          try {
             val message=new ProducerRecord[String, String](topicName,"partition0-key", line)
             println(line)
             producer.send(message)
          } catch {
            case ex: Exception => {
              System.err.println(ex.getMessage, ex)
            }
          }
        })
      })
       
      println ("------------------------------- PRODUCER END -------------------------------")
      
   }
}