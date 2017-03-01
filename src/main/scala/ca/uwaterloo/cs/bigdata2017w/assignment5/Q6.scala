package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

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

    val date = sc.broadcast(args.date())

    if (args.text()) {
      val lineitems = sc.textFile(args.input() + "/lineitem.tbl")

      val result = lineitems
        .filter(line => line.split('|')(10).substring(0, date.value.length()) == date.value)
        .map(line => {
          val cs = line.split('|')
          val l_quantity = cs(4).toInt
          val l_extendedprice = cs(5).toDouble
          val l_discount = cs(6).toDouble
          val l_tax = cs(7).toDouble
          val disc_price = l_extendedprice*(1.0-l_discount)
          val charge = l_extendedprice*(1.0-l_discount)*(1.0+l_tax)
          ((cs(8), cs(9)), (l_quantity, l_extendedprice, disc_price, charge, l_discount, 1))  
        }).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4, v1._5 + v2._5, v1._6 + v2._6))
        .collect()

      for (i <- 0 until result.length) {
          val pair1 = result(i)._1
          val pair2 = result(i)._2

          val l_returnflag = pair1._1
          val l_linestatus = pair1._2

          val sum_qty = pair2._1
          val sum_base_price = pair2._2
          val sum_disc_price = pair2._3
          val sum_charge = pair2._4
          val count = pair2._6
          val avg_qty = sum_qty.toDouble / count
          val avg_price = sum_base_price / count
          val avg_disc = pair2._5 / count

          println(s"($l_returnflag,$l_linestatus,$sum_qty,$sum_base_price,$sum_disc_price,$sum_charge,$avg_qty,$avg_price,$avg_disc,$count)")
      }

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      // c_custkey -> c_nationkey
      val customerDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/customer")
      val customerRDD = customerDF.rdd
      val customerMap = sc.broadcast(customerRDD
        .map(row => (row(0).toString, row(3).toString))
        .collectAsMap)

      // n_nationkey -> n_name
      val nationDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/nation")
      val nationRDD = nationDF.rdd
      val nationMap = sc.broadcast(nationRDD
        .map(row => (row(0).toString, row(1).toString))
        .collectAsMap)

      val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val lineitemRDD = lineitemDF.rdd

      val result = lineitemRDD
        .filter(row => row(10).toString.substring(0, date.value.length()) == date.value)
        .map(cs => {
          val l_quantity = cs(4).toString.toDouble.toInt
          val l_extendedprice = cs(5).toString.toDouble
          val l_discount = cs(6).toString.toDouble
          val l_tax = cs(7).toString.toDouble
          val disc_price = l_extendedprice*(1.0-l_discount)
          val charge = l_extendedprice*(1.0-l_discount)*(1.0+l_tax)
          ((cs(8).toString, cs(9).toString), (l_quantity, l_extendedprice, disc_price, charge, l_discount, 1))  
        }).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4, v1._5 + v2._5, v1._6 + v2._6))
        .collect()

      for (i <- 0 until result.length) {
          val pair1 = result(i)._1
          val pair2 = result(i)._2

          val l_returnflag = pair1._1
          val l_linestatus = pair1._2

          val sum_qty = pair2._1
          val sum_base_price = pair2._2
          val sum_disc_price = pair2._3
          val sum_charge = pair2._4
          val count = pair2._6
          val avg_qty = sum_qty.toDouble / count
          val avg_price = sum_base_price / count
          val avg_disc = pair2._5 / count

          println(s"($l_returnflag,$l_linestatus,$sum_qty,$sum_base_price,$sum_disc_price,$sum_charge,$avg_qty,$avg_price,$avg_disc,$count)")
      }

    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
