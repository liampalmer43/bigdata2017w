package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.math.exp
import scala.collection.mutable.Map

class A6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output path", required = true)
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A6Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input())

    val trained = textFile.map(line => {
      val elements = line.split("\\s+")
      (0, (elements(0), elements(1), elements.slice(2, elements.length).map(x => x.toInt)))
    }).groupByKey(1)
    .flatMap(keyIterablePair => {
      val iter  = keyIterablePair._2.iterator
      val w = Map[Int, Double]()
      val delta = 0.002
println(iter.hasNext)
      while (iter.hasNext) {
        val data = iter.next
        val isSpam = if (data._2 == "spam") 1 else 0

        // Compute score
        var score = 0.0d
        data._3.foreach(b => if (w.contains(b)) score = score + w(b))

        // Update w
        val prob = 1.0 / (1 + exp(-score))
        data._3.foreach(b => {
          if (w.contains(b)) {
            w(b) += (isSpam - prob) * delta
          } else {
            w(b) = (isSpam - prob) * delta
          }
        })
      }
      w.toList.map(p => {
        val b = p._1
        val score = p._2
        s"($b,$score)"
      })
    }).saveAsTextFile(args.model())
  }
}
