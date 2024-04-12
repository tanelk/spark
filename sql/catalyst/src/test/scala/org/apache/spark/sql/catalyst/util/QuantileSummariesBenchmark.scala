/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.util

import scala.util.Random

import org.apache.datasketches.kll.{KllDoublesSketch, KllSketch}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for the QuantileSummaries class.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/QuantileSummariesBenchmark-results.txt".
 * }}}
 */
object QuantileSummariesBenchmark extends BenchmarkBase {

  def test(name: String, numValues: Int): Unit = {
    runBenchmark(name) {
      val values = (1 to numValues).map(_ => Random.nextDouble())

      val benchmark = new Benchmark(name, numValues, output = output)
      benchmark.addCase("Only insert") { _: Int =>
        var summaries = new QuantileSummaries(
          compressThreshold = QuantileSummaries.defaultCompressThreshold,
          relativeError = QuantileSummaries.defaultRelativeError)

        for (value <- values) {
          summaries = summaries.insert(value)
        }
        summaries = summaries.compress()

        println("Median: " + summaries.query(0.5))
      }

      benchmark.addCase("Insert & merge") { _: Int =>
        // Insert values in batches of 1000 and merge the summaries.
        val summaries = values.grouped(1000).map(vs => {
          var partialSummaries = new QuantileSummaries(
            compressThreshold = QuantileSummaries.defaultCompressThreshold,
            relativeError = QuantileSummaries.defaultRelativeError)

          for (value <- vs) {
            partialSummaries = partialSummaries.insert(value)
          }
          partialSummaries.compress()
        }).reduce(_.merge(_))

        println("Median: " + summaries.query(0.5))
      }

      benchmark.addCase("KllFloatsSketch insert") { _: Int =>
        // Insert values in batches of 1000 and merge the summaries.
        val summaries = KllDoublesSketch.newHeapInstance(
          KllSketch.getKFromEpsilon(QuantileSummaries.defaultRelativeError, true)
        )

        for (value <- values) {
          summaries.update(value)
        }

        println("Median: " + summaries.getQuantile(0.5))
      }

      benchmark.addCase("KllFloatsSketch Insert & merge") { _: Int =>
        // Insert values in batches of 1000 and merge the summaries.
        val summaries = values.grouped(1000).map(vs => {
          val partialSummaries = KllDoublesSketch.newHeapInstance(
            KllSketch.getKFromEpsilon(QuantileSummaries.defaultRelativeError, true)
          )

          for (value <- vs) {
            partialSummaries.update(value)
          }

          partialSummaries
        }).reduce((a, b) => {
          a.merge(b)
          a
        })

        println("Median: " + summaries.getQuantile(0.5))
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    test("QuantileSummaries", 1_000_000)
  }
}
