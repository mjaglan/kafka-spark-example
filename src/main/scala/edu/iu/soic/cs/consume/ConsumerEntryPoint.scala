package edu.iu.soic.cs.consume

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

import kafka.serializer._
import kafka.utils._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.io._
import scala.swing.Orientable

// Consumer
class  ConsumerEntryPoint
object ConsumerEntryPoint {
   def main(args: Array[String]) {
     
      println("------------------------------- CONSUMER -------------------------------")
     
      if(args.length > 0 && args(0)=="--help") {
          println("arguments[3]: subscriberTopicList  ZooKeeperServer  kafkaBroker")
           System.exit(0)
      }
      
      // get subscriberTopicList  ZooKeeperServer  kafkaBroker
      val topicsSet = args(0).split(",").toSet
      val zooKeeperServer = args(1)
      val kafkaBroker = args(2)
      
      val conf = new SparkConf().
      setAppName("consumer-spark-kafka-demo-app").
      setMaster("local[*]"). // for local testing
      set("spark.driver.allowMultipleContexts", "false").
      set("spark.streaming.stopGracefullyOnShutdown","true")


      val sc  = SparkContext.getOrCreate(conf)

      // Read data streams from Kafka!
      val ssc = new StreamingContext(sc, Seconds(2))

      // Create direct kafka stream with brokers and topics
      val kafkaParams = Map[String, String](
          "metadata.broker.list"            -> kafkaBroker,
          "zookeeper.connect"               -> zooKeeperServer,
          "group.id"                        -> "group-1",
          "zookeeper.connection.timeout.ms" -> "1000")
          
      val kafkaInputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      
      val outDirName = "hdfs://localhost:54310//user/ddos/output"

      val APPROACH = 1
      
      if (APPROACH==0) { // Sequential Approach - print DDOS IP to STDOUT
        /*
        kafkaInputDStream.foreachRDD { anRDD =>
          var checkmap = scala.collection.mutable.Map[String,String]()
          var ddosIP = scala.collection.mutable.Set[String]()
          
          // Complex map expression: http://stackoverflow.com/a/29473163
          val newRDD = anRDD.map ( line => { 
                  val msg = line._2
                  val msgList = msg.split(" ")
                  val keyIP = msgList(0).trim()
                  val valTS = msgList(3).trim().substring(1)
                  
                  // Main DDOS detect logic
                  if (checkmap.contains(keyIP)) {
                    // Simple Logic: two simultaneous hits from same source are dos attack
                    if (checkmap.get(keyIP).get == valTS) {
                      ddosIP += keyIP

                      if (ddosIP.contains(keyIP)) {
                        println (keyIP)
                      }
                      
                    } else {
                      checkmap.update(keyIP, valTS)
                    }
                  } else {
                    checkmap.put(keyIP,valTS)
                  }
                  
          }) // anRDD.map
                    
        }  // for each RDD  
        */
        println("-------------- Sequential Approach --------------")
        
     } else if (APPROACH==1) { // Approach: Write RDD to HDFS
          
          kafkaInputDStream.foreachRDD { anRDD =>
            // tuple2 => (IP, TS)
            val x1 = anRDD.map { line => (line._2.split(" ").map { e => e.trim() }.array(0), line._2.split(" ").map { e => e.trim() }.array(3).substring(1)) }
            
            // tuple2 => ((IP, TS), 1)
            val x2 = x1.map(line => (line, 1))
            
            // tuple2 => MAP ( KEY(IP, TS), value(n) )
            val x3 = x2.reduceByKey(_ + _).collectAsMap()
            
            // tuple2 => MAP ( KEY(IP, TS), value(n) ), get DDOS IP
            val x4 = x2.filter(aTuple => x3.get(aTuple._1).get > 1 )
            
            // tuple2 => IP
            val x5 = x4.map(aTuple => aTuple._1._1)
            
            // Return a new RDD containing the distinct elements
            val x6 = x5.coalesce(1, true).distinct()
            
            // print IP
            x6.foreach { x => println(x) }
            
            // save RDD
            val prefix = outDirName
            val TIME_IN_MS = System.currentTimeMillis().toString()
            x6.saveAsTextFile(prefix + "-" + TIME_IN_MS)

          } // for each RDD  
          
      } else if (APPROACH==2) { // Approach: Write DStream to HDFS
        
          val X = kafkaInputDStream.transform( anRDD => {
            // tuple2 => (IP, TS)
            val x1 = anRDD.map { line => (line._2.split(" ").map { e => e.trim() }.array(0), line._2.split(" ").map { e => e.trim() }.array(3).substring(1)) }
            
            // tuple2 => ((IP, TS), 1)
            val x2 = x1.map(line => (line, 1))
            
            // tuple2 => MAP ( KEY(IP, TS), value(n) )
            val x3 = x2.reduceByKey(_ + _).collectAsMap()
            
            // tuple2 => MAP ( KEY(IP, TS), value(n) ), get DDOS IP
            val x4 = x2.filter(aTuple => x3.get(aTuple._1).get > 1 )
            
            // tuple2 => IP
            val x5 = x4.map(aTuple => aTuple._1._1)
            
            // Return a new RDD containing the distinct elements
            val x6 = x5.coalesce(1, true).distinct()
            
            x6

          }) // for each RDD  
          
          // print IP
          X.foreachRDD(r => { r.foreach { x => println(x) }}) 
          
          // save DStream
          X.saveAsTextFiles(outDirName, "dir") 
          
      } else {
        
        println("-------------- Nothing --------------")
      }
      
      
    // checkpoint the DStream operations for driver fault-tolerance 
    ssc.checkpoint("checkpoint-consume")
    
    // Start the computation
    // READ: http://why-not-learn-something.blogspot.dk/2016/05/apache-spark-streaming-how-to-do.html
    ssc.start()
    ssc.awaitTermination()
    
   }

}


