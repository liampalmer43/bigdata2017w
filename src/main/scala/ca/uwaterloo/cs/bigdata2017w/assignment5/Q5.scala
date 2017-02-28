package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.collection.mutable.ListBuffer

class A5PlainConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, reducers)
  val input = opt[String](descr = "input path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val text = opt[Boolean](descr = "text option", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet option", required = false, default = Some(false))
  verify()
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A5PlainConf(argv)

    log.info("Input: " + args.input())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val outputName = "A5Q5"
    val outputDir = new Path(outputName)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    if (args.text()) {
      // c_custkey -> c_nationkey
      val customers = sc.textFile(args.input() + "/customer.tbl")
      val customerMap = sc.broadcast(customers
        .map(line => (line.split('|')(0), line.split('|')(3)))
        .collectAsMap)

      // n_nationkey -> n_name
      val nations = sc.textFile(args.input() + "/nation.tbl")
      val nationMap = sc.broadcast(nations
        .map(line => (line.split('|')(0), line.split('|')(1)))
        .collectAsMap)

      val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")

      val lineitemData = lineitems
        .filter(line => line.split('|')(10).substring(0, date.value.length()) == date.value)
        .map(line => (line.split('|')(0), line.split('|')(10).substring(0, 7)))

      val orderData = orders
        .map(line => (line.split('|')(0), line.split('|')(1)))

      val result = lineitemData.cogroup(orderData)
        .filter(keyIterablePair => keyIterablePair._2._1.iterator.hasNext)
        .flatMap(keyIterablePair => {
          // We have (orderKey, (Iter["date"], Iter[custKey]))
          if (keyIterablePair._2._2.iterator.length != 1) {
            throw new IllegalArgumentException("Only one valid custKey for an order in the order table")
          }
          val custKey = keyIterablePair._2._2.iterator.next;
          // c_custkey -> c_nationkey
          // n_nationkey -> n_name
          val nationKey = customerMap.value(custKey)
          val nationName = nationMap.value(nationKey)

          val iter = keyIterablePair._2._1.iterator
          var result = new ListBuffer[((String, String), Int)]()
          while (iter.hasNext) {
            val e = ((nationName, iter.next), 1)
            result += e
          }
          result.toList
        }).reduceByKey(_ + _)
        .filter(pair => pair._1._1 == "Canada" || pair._1._1 == "USA")
        .sortByKey(numPartitions = 1)
        .map(pair => {
          val nationName = pair._1._1
          val date = pair._1._2
          val count = pair._2._2
          println(s"($nationName,$date,$count)")
          pair
        })

      result.saveAsTextFile(outputName)

    } else if (args.parquet()) {

    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
