package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.collection.mutable.ListBuffer

class A5Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, reducers, date)
  val input = opt[String](descr = "input path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val text = opt[Boolean](descr = "text option", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet option", required = false, default = Some(false))
  val date = opt[String](descr = "date filter", required = true)
  verify()
}

object Q1 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = sc.broadcast(args.date())

    if (args.text()) {
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      val result = textFile
        .filter(line => line.split('|')(10).substring(0, date.value.length()) == date.value)
        .count()
      println("ANSWER=" + result)
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val result = lineitemRDD
        .filter(row => row(10).toString.substring(0, date.value.length()) == date.value)
        .count()
      println("ANSWER=" + result)
    } else {
      throw new IllegalArgumentException("Must include at least one of --text or --parquet command line options")
    }
  }
}
