package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    val date = sc.broadcast(args.date())

    if (args.text()) {
      val parts = sc.textFile(args.input() + "/part.tbl")
      val partMap = sc.broadcast(parts
        .map(line => (line.split('|')(0), line.split('|')(1)))
        .collectAsMap)

      val suppliers = sc.textFile(args.input() + "/supplier.tbl")
      val supplierMap = sc.broadcast(suppliers
        .map(line => (line.split('|')(0), line.split('|')(1)))
        .collectAsMap)
      
      val lineitems = sc.textFile(args.input() + "/lineitem.tbl")

      val result = lineitems
        .filter(line => line.split('|')(10).substring(0, date.value.length()) == date.value)
        .map(line => (line.split('|')(0), (partMap.value(line.split('|')(1)), supplierMap.value(line.split('|')(2)))))
        .takeOrdered(20)(Ordering[Int].on { (pair: (String, (String, String))) => Integer.parseInt(pair._1) })

      for (i <- 0 until result.length) {
          val pair = result(i)
          val l = pair._1
          val p = pair._2._1
          val s = pair._2._2
          println(s"($l,$p,$s)")
      }

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd

      val partMap = sc.broadcast(partRDD
        .map(row => (row(0).toString, row(1).toString))
        .collectAsMap)

      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd

      val supplierMap = sc.broadcast(supplierRDD
        .map(row => (row(0).toString, row(1).toString))
        .collectAsMap)

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      val result = lineitemRDD
        .filter(row => row(10).toString.substring(0, date.value.length()) == date.value)
        .map(row => (row(0).toString, (partMap.value(row(1).toString), supplierMap.value(row(2).toString))))
        .takeOrdered(20)(Ordering[Int].on { (pair: (String, (String, String))) => Integer.parseInt(pair._1) })

      for (i <- 0 until result.length) {
          val pair = result(i)
          val l = pair._1
          val p = pair._2._1
          val s = pair._2._2
          println(s"($l,$p,$s)")
      }

    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
