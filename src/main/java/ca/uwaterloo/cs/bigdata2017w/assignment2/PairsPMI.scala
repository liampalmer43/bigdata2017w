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

import scala.collection.mutable.ListBuffer

class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "count threshold", required = false, default = Some(10))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

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
        var uniquePairs = new ListBuffer[(String, String)]()
        val list = uniqueWords.toList
        for (i <- 0 to list.length-1) {
          for (j <- 0 to list.length-1) {
            if (i != j) {
              val pair = (list(i), list(j))
              uniquePairs += pair
            }
          }
        }
        uniquePairs.toList
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter(x => x._2 >= threshold.value)
      .map(x => {
        val pair = x._1
        val num = x._2
        val pmi = Math.log10(counts.value("*")*num.toFloat/(counts.value(pair._1)*counts.value(pair._2)))
        (pair, (pmi.toFloat, num))
      })
      .map(result => {
        val word1 = result._1._1
        val word2 = result._1._2
        val pmi = result._2._1
        val count = result._2._2
        s"($word1, $word2) ($pmi, $count)"
      })

    result.saveAsTextFile(args.output())
  }
}
