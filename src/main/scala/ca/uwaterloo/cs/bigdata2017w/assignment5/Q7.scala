package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    val date = sc.broadcast(args.date())

    if (args.text()) {
      // c_custkey -> c_name
      val customers = sc.textFile(args.input() + "/customer.tbl")
      val customerMap = sc.broadcast(customers
        .map(line => (line.split('|')(0), line.split('|')(1)))
        .collectAsMap)

      val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")

      val lineitemData = lineitems
        .filter(line => line.split('|')(10) > date.value)
        .map(line => {
          val cs = line.split('|')
          val l_extendedprice = cs(5).toDouble
          val l_discount = cs(6).toDouble
          val revenue = l_extendedprice*(1.0-l_discount)
          (line.split('|')(0), revenue)
        })

      val orderData = orders
        .filter(line => line.split('|')(4) < date.value)
        .map(line => (line.split('|')(0), (line.split('|')(1), line.split('|')(4), line.split('|')(7))))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext && keyIterablePair._2._2.iterator.hasNext)
        .map(keyIterablePair => {
          // We have (orderKey, (Iter[revenue], Iter[(custKey, orderDate, shipPriority)]))
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid set of order data for an order in the order table")
          }

          val orderKey = keyIterablePair._1

          val orderData = keyIterablePair._2._2.iterator.next;
          val custKey = orderData._1
          val orderDate = orderData._2
          val shipPriority = orderData._3
          // c_custkey -> c_name
          val custName = customerMap.value(custKey)
          
          val iter = keyIterablePair._2._1.iterator
          var totalRevenue = 0.0
          while (iter.hasNext) {
            totalRevenue = totalRevenue + iter.next
          }

          ((custName, orderKey, orderDate, shipPriority), totalRevenue)
        }).reduceByKey(_ + _)
        .map(pair => (pair._2, pair._1))
        .sortByKey(false, 1)
        .takeOrdered(10)(Ordering[Double].reverse.on { (pair: (Double, (String, String, String, String))) => pair._1 })

      for (i <- 0 until result.length) {
          val pair = result(i)
          val c_name = pair._2._1
          val l_orderkey = pair._2._2
          val revenue = pair._1
          val o_orderdate = pair._2._3
          val o_shippriority = pair._2._4
          println(s"($c_name,$l_orderkey,$revenue,$o_orderdate,$o_shippriority)")
      }

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      // c_custkey -> c_name
      val customerDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/customer")
      val customerRDD = customerDF.rdd
      val customerMap = sc.broadcast(customerRDD
        .map(row => (row(0).toString, row(1).toString))
        .collectAsMap)

      val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val lineitemRDD = lineitemDF.rdd

      val orderDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/orders")
      val orderRDD = orderDF.rdd

      val lineitemData = lineitemRDD
        .filter(row => row(10).toString > date.value)
        .map(cs => {
          val l_extendedprice = cs(5).toString.toDouble
          val l_discount = cs(6).toString.toDouble
          val revenue = l_extendedprice*(1.0-l_discount)
          (cs(0).toString, revenue)
        })

      val orderData = orderRDD
        .filter(row => row(4).toString < date.value)
        .map(row => (row(0).toString, (row(1).toString, row(4).toString, row(7).toString)))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext && keyIterablePair._2._2.iterator.hasNext)
        .map(keyIterablePair => {
          // We have (orderKey, (Iter[revenue], Iter[(custKey, orderDate, shipPriority)]))
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid set of order data for an order in the order table")
          }

          val orderKey = keyIterablePair._1

          val orderData = keyIterablePair._2._2.iterator.next;
          val custKey = orderData._1
          val orderDate = orderData._2
          val shipPriority = orderData._3
          // c_custkey -> c_name
          val custName = customerMap.value(custKey)
          
          val iter = keyIterablePair._2._1.iterator
          var totalRevenue = 0.0
          while (iter.hasNext) {
            totalRevenue = totalRevenue + iter.next
          }

          ((custName, orderKey, orderDate, shipPriority), totalRevenue)
        }).reduceByKey(_ + _)
        .map(pair => (pair._2, pair._1))
        .sortByKey(false, 1)
        .takeOrdered(10)(Ordering[Double].reverse.on { (pair: (Double, (String, String, String, String))) => pair._1 })

      for (i <- 0 until result.length) {
          val pair = result(i)
          val c_name = pair._2._1
          val l_orderkey = pair._2._2
          val revenue = pair._1
          val o_orderdate = pair._2._3
          val o_shippriority = pair._2._4
          println(s"($c_name,$l_orderkey,$revenue,$o_orderdate,$o_shippriority)")
      }

    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
