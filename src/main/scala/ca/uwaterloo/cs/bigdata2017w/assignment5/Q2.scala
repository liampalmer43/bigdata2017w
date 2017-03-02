package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.collection.mutable.ListBuffer

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val date = sc.broadcast(args.date())

    if (args.text()) {
      val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")

      val lineitemData = lineitems
        .filter(line => line.split('|')(10).substring(0, date.value.length()) == date.value)
        .map(line => (line.split('|')(0), '*'))

      val orderData = orders
        .map(line => (line.split('|')(0), line.split('|')(6)))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext)
        .flatMap(keyIterablePair => {
          // (orderKey, Iter['*'], Iter[clerk])
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid clerk for an order in the order table")
          }
          val orderKey = keyIterablePair._1
          val clerk = keyIterablePair._2._2.iterator.next
          var result = new ListBuffer[(String, String)]()
          val iter = keyIterablePair._2._1.iterator
          while (iter.hasNext) {
            iter.next
            val e = (clerk, orderKey)
            result += e
          }
          result.toList
        }).takeOrdered(20)(Ordering[Int].on { (pair: (String, String)) => Integer.parseInt(pair._2) })

      for (i <- 0 until result.length) {
          val pair = result(i)
          val p1 = pair._1
          val p2 = pair._2
          println(s"($p1,$p2)")
      }

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitemData = lineitemRDD
        .filter(row => row(10).toString.substring(0, date.value.length()) == date.value)
        .map(row => (row(0).toString, '*'))

      val orderDF = sparkSession.read.parquet(args.input() + "/orders")
      val orderRDD = orderDF.rdd
      val orderData = orderRDD
        .map(row => (row(0).toString, row(6).toString))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext)
        .flatMap(keyIterablePair => {
          // (orderKey, Iter['*'], Iter[clerk])
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid clerk for an order in the order table")
          }
          val orderKey = keyIterablePair._1
          val clerk = keyIterablePair._2._2.iterator.next
          var result = new ListBuffer[(String, String)]()
          val iter = keyIterablePair._2._1.iterator
          while (iter.hasNext) {
            iter.next
            val e = (clerk, orderKey)
            result += e
          }
          result.toList
        }).takeOrdered(20)(Ordering[Int].on { (pair: (String, String)) => Integer.parseInt(pair._2) })

      for (i <- 0 until result.length) {
          val pair = result(i)
          val p1 = pair._1
          val p2 = pair._2
          println(s"($p1,$p2)")
      }
    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
