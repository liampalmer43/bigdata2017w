package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class A6ApplyEnsembleConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "ensemble method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A6ApplyEnsembleConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val method = sc.broadcast(args.method())

    val modelX = sc.textFile(args.model() + "/part-00000")
    val modelY = sc.textFile(args.model() + "/part-00001")
    val modelB = sc.textFile(args.model() + "/part-00002")

    val weightsX = sc.broadcast(modelX
      .map(line => {
        val kv = line.slice(1,line.length-1).split(",")
        (kv(0).toInt, kv(1).toDouble)
      })
      .collectAsMap)

    val weightsY = sc.broadcast(modelY
      .map(line => {
        val kv = line.slice(1,line.length-1).split(",")
        (kv(0).toInt, kv(1).toDouble)
      })
      .collectAsMap)

    val weightsB = sc.broadcast(modelB
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
      var scoreX = 0.0d
      var scoreY = 0.0d
      var scoreB = 0.0d
      elements.slice(2, elements.length).map(x => x.toInt).foreach(b => {
        if (weightsX.value.contains(b)) scoreX = scoreX + weightsX.value(b)
        if (weightsY.value.contains(b)) scoreY = scoreY + weightsY.value(b)
        if (weightsB.value.contains(b)) scoreB = scoreB + weightsB.value(b)
      })
      if (method.value == "average") {
        val score = (scoreX + scoreY + scoreB) / 3
        val prediction = if (score > 0.0d) "spam" else "ham"
        s"($id,$actual,$score,$prediction)"
      } else if (method.value == "vote") {
        val score = (if (scoreX > 0.0d) 1 else -1) + (if (scoreY > 0.0d) 1 else -1) + (if (scoreB > 0.0d) 1 else -1)
        val prediction = if (score > 0) "spam" else "ham"
        s"($id,$actual,$score,$prediction)"
      } else {
        throw new IllegalArgumentException("Must specify either average or vote as method parameter")
      }
    }).saveAsTextFile(args.output())
  }
}
