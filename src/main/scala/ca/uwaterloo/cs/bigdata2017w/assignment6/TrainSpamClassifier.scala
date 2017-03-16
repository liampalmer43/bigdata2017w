package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.math.exp
import scala.collection.mutable.Map
import scala.util.Random

class A6TrainConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle option", required = false, default = Some(false))
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A6TrainConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    if (args.shuffle()) {
      val trained = textFile.map(line => {
        val elements = line.split("\\s+")
        (Random.nextFloat, (elements(0), elements(1), elements.slice(2, elements.length).map(x => x.toInt)))
      }).sortByKey(numPartitions = 1)
      .map(p => (0, p._2))
      .groupByKey(1)
      .flatMap(keyIterablePair => {
        val iter  = keyIterablePair._2.iterator
        val w = Map[Int, Double]()
        val delta = 0.002
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

    } else {
      val trained = textFile.map(line => {
        val elements = line.split("\\s+")
        (0, (elements(0), elements(1), elements.slice(2, elements.length).map(x => x.toInt)))
      }).groupByKey(1)
      .flatMap(keyIterablePair => {
        val iter  = keyIterablePair._2.iterator
        val w = Map[Int, Double]()
        val delta = 0.002
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
}
