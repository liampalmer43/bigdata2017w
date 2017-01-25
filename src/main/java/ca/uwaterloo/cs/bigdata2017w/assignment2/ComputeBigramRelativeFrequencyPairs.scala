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

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val pairs = if (tokens.length > 1) tokens.sliding(2).map(p => (p(0), p(1))).toList else List()
        val singles = tokens.dropRight(1).map(w => (w, "*"))
        singles ::: pairs
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .groupBy(x => x._1._1)
      .flatMap(x => {
        val word = x._1
        val iter = x._2
        val (iter1,iter2) = iter.iterator.duplicate
        var wordCount = -1
        var found = false
        while (iter1.hasNext && !found) {
          val pair = iter1.next
          if (pair._1._2 == "*") {
            wordCount = pair._2
            found = true;
          }
        }
        var result = new ListBuffer[((String, String), Float)]()
        while (iter2.hasNext) {
          val pair = iter2.next
          if (pair._1._2 != "*") {
            val p = (pair._1, pair._2/wordCount.toFloat)
            result += p
          } else {
            val p = (pair._1, pair._2.toFloat)
            result += p
          }
        }
        result.toList
      }).map(pair => {
        val word1 = pair._1._1
        val word2 = pair._1._2
        val count = pair._2
        s"($word1, $word2) $count"
      })
    counts.saveAsTextFile(args.output())
  }
}
