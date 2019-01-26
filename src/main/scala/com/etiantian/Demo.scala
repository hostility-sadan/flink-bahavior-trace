package com.etiantian

import org.apache.flink.api.scala._

/**
 * Hello world!
 *
 */
object Demo {
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements[String]("Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { (_, 2) }
        .groupBy(0)
      .sum(1)

    // print the results with a single thread, rather than in parallel
    windowCounts.print()

  }
}
