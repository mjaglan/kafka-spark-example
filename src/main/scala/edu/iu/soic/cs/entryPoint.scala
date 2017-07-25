package edu.iu.soic.cs

import edu.iu.soic.cs.consume._
import edu.iu.soic.cs.produce._

class  entryPoint
object entryPoint {
     def main(args: Array[String]) {
       if (args.length < 1) {
         println("Help:")
         println("java -jar ./build/libs/kafka.spark-all.jar <arg0> <arg1> <arg2> <arg3> <arg4> <arg5>")
         System.exit(-1)
       
       } else if (args.length == 1) {
         println("------------------- DEBUG MODE ---------------------")
         
         if (args(0) == "produce") {
           ProducerEntryPoint.main(Array("1","topic1","hdfs://localhost:54310//user/ddos/input/test_input.large","localhost:9092","localhost:2181"))
           
         } else if (args(0) == "consume") {
           ConsumerEntryPoint.main(Array("topic1","localhost:2181","localhost:9092"))
           
         } else {
           println("ERROR: wrong argument(s)" + args.map { x =>println(x) })
           System.exit(-1)
         }
         
       } else {
         println("------------------- RELEASE MODE -------------------")
         
         if (args(0) == "produce") {
           ProducerEntryPoint.main(Array(args(1),args(2),args(3),args(4),args(5)))
           
         } else if (args(0) == "consume") {
           ConsumerEntryPoint.main(Array(args(1),args(2),args(3)))
           
         } else {
           println("ERROR: wrong argument(s)" + args.map { x =>println(x) })
           System.exit(-1)
         }
       }
     }
}
