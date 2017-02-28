package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.collection.mutable.ListBuffer

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val outputName = "A5Q6"
    val outputDir = new Path(outputName)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val date = sc.broadcast(args.date())

    if (args.text()) {
      val lineitems = sc.textFile(args.input() + "/lineitem.tbl")

      val lineitemData = lineitems
        .filter(line => line.split('|')(10).substring(0, date.value.length()) == date.value)
        // Condense line data:
        .map(line => 
        .map(line => (line.split('|')(0), '*'))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext)
        .map(keyIterablePair => {
          // We have (orderKey, (Iter["*"], Iter[custKey]))
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid custKey for an order in the order table")
          }
          val custKey = keyIterablePair._2._2.iterator.next;
          // c_custkey -> c_nationkey
          // n_nationkey -> n_name
          val nationKey = customerMap.value(custKey)
          val nationName = nationMap.value(nationKey)
          ((nationKey, nationName), keyIterablePair._2._1.iterator.length)          
        }).reduceByKey(_ + _)
        .map(pair => (Integer.parseInt(pair._1._1), (pair._1._2, pair._2)))
        .sortByKey(numPartitions = 1)
        .map(pair => {
          val nationKey = pair._1
          val nationName = pair._2._1
          val count = pair._2._2
          println(s"($nationKey,$nationName,$count)")
          pair
        })

      result.saveAsTextFile(outputName)

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      // c_custkey -> c_nationkey
      val customerDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/customer")
      val customerRDD = customerDF.rdd
      val customerMap = sc.broadcast(customerRDD
        .map(row => (row(0), row(3)))
        .collectAsMap)

      // n_nationkey -> n_name
      val nationDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/nation")
      val nationRDD = nationDF.rdd
      val nationMap = sc.broadcast(nationRDD
        .map(row => (row(0), row(1)))
        .collectAsMap)

      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitemData = lineitemRDD
        .filter(row => row(10).toString.substring(0, date.value.length()) == date.value)
        .map(row => (row(0).toString, '*'))

      val orderDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/orders")
      val orderRDD = orderDF.rdd
      val orderData = orderRDD
        .map(row => (row(0).toString, row(1).toString))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext)
        .map(keyIterablePair => {
          // We have (orderKey, (Iter["*"], Iter[custKey]))
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid custKey for an order in the order table")
          }
          val custKey = keyIterablePair._2._2.iterator.next;
          // c_custkey -> c_nationkey
          // n_nationkey -> n_name
          val nationKey = customerMap.value(custKey)
          val nationName = nationMap.value(nationKey)
          ((nationKey, nationName), keyIterablePair._2._1.iterator.length)          
        }).reduceByKey(_ + _)
        .map(pair => (Integer.parseInt(pair._1._1), (pair._1._2, pair._2)))
        .sortByKey(numPartitions = 1)
        .map(pair => {
          val nationKey = pair._1
          val nationName = pair._2._1
          val count = pair._2._2
          println(s"($nationKey,$nationName,$count)")
          pair
        })

      result.saveAsTextFile(outputName)

    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
