/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "count threshold", required = false, default = Some(10))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val threshold = sc.broadcast(args.threshold())

    // Initialize broadcast variable map.
    val counts = sc.broadcast(textFile
      .flatMap(line => {
        var words = Set[String]()
        var uniqueWords = new ListBuffer[String]()
        val tokens = tokenize(line)
        for (i <- 0 to Math.min(tokens.length-1, 39)) {
          val word = tokens(i)
          if (!words.contains(word)) {
            uniqueWords += word
            words += word
          }
        }
        uniqueWords += "*"
        uniqueWords.toList
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collectAsMap)

    val result = textFile
      .flatMap(line => {
        var words = Set[String]()
        var uniqueWords = new ListBuffer[String]()
        val tokens = tokenize(line)
        for (i <- 0 to Math.min(tokens.length-1, 39)) {
          val word = tokens(i)
          if (!words.contains(word)) {
            uniqueWords += word
            words += word
          }
        }
        var uniquePairs = new ListBuffer[(String, Map[String, Int])]()
        val list = uniqueWords.toList
        for (i <- 0 to list.length-1) {
          var stripe = Map[String, Int]()
          for (j <- 0 to list.length-1) {
            if (i != j) {
              stripe += (list(j) -> 1)
            }
          }
          val pair = (list(i), stripe)
          uniquePairs += pair
        }
        uniquePairs.toList
      })
      .reduceByKey((map1, map2) => {
        map1 ++ map2.map{case (k,v) => k -> (v + map1.getOrElse(k,0))}
      })
      .map(x => {
        var word = x._1
        var map = x._2
        for ((k, v) <- x._2) {
          if (v < threshold.value) {
            map -= k
          }
        }
        (word, map)
      })
      .filter(x => x._2.size > 0)
      .map(x => {
        val word = x._1
        val map = Map[String, (Float, Int)]()
        for ((k, v) <- x._2) {
          val entry = (k, (Math.log10(counts.value("*")*v.toFloat/(counts.value(word)*counts.value(k))).toFloat, v))
          map += entry
        }
        (word, map)
      })
      .map(result => {
        val word = result._1
        val map = result._2.map(kv => kv._1 + "=" + "(" + kv._2._1.toString + "," + kv._2._2.toString + ")").mkString("{", ", ", "}")
        s"$word $map"
      })

    result.saveAsTextFile(args.output())
  }
}
