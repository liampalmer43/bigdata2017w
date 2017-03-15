package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.math.exp

class A6ApplyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "output path", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A6ApplyConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = sc.textFile(args.model() + "/part*")
    val weights = sc.broadcast(model
      .map(line => {
        val kv = line.slice(1,line.length-1).split(",")
        (kv(0).toInt, kv(1).toDouble)
      })
      .collectAsMap)

    val textFile = sc.textFile(args.input())

    val result = textFile.map(line => {
      val elements = line.split("\\s+")
      val id = elements(0)
      val actual = elements(1)
      var score = 0.0d
      elements.slice(2, elements.length).map(x => x.toInt).foreach(b => if (weights.value.contains(b)) score = score + weights.value(b))
      val prediction = if (score > 0.0d) "spam" else "ham"
      s"($id,$actual,$score,$prediction)"
    }).saveAsTextFile(args.output())
  }
}
