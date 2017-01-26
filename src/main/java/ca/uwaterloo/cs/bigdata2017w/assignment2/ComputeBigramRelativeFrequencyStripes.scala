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

class StripesConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        var words = Map[String, Map[String, Float]]()
        for (i <- 0 to tokens.length-2) {
          val word1 = tokens(i)
          val word2 = tokens(i+1)
          if (words.contains(word1)) {
            if (words(word1).contains(word2)) {
              var map = words(word1)
              map += (word2 -> (map(word2) + 1.0f))
              words += (word1 -> map)
            } else {
              var map = words(word1)
              map += (word2 -> 1.0f)
              words += (word1 -> map)
            }
          } else {
            var map = Map[String, Float]()
            map += (word2 -> 1.0f)
            words += (word1 -> map)
          }
        }
        var result = new ListBuffer[(String, Map[String, Float])]
        for ((k, v) <- words) {
          val pair = (k, v)
          result += pair
        }
        result.toList
      })
      .reduceByKey((map1, map2) => {
        map1 ++ map2.map{case (k,v) => k -> (v + map1.getOrElse(k,0.0f))}
      })
      .map(s => {
        var map = Map[String, Float]()
        var sum = 0.0f
        for ((k,v) <- s._2) sum += v
        for ((k,v) <- s._2) map += (k -> v/sum)
        val wordString = s._1
        val mapString = map.map(kv => kv._1 + "=" + kv._2.toString).mkString("{", ", ", "}")
        s"$wordString $mapString"
      })
    counts.saveAsTextFile(args.output())
  }
}
