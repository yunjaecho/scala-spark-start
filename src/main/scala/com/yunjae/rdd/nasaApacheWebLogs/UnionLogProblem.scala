package com.yunjae.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem extends App {
  val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val julyFirstLog = sc.textFile("in/nasa_19950701.tsv")
  val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

  val aggregatedLogLines = julyFirstLog.union(augustFirstLogs)

  val cleanLOgLines = aggregatedLogLines.filter(line => isNotHeader(line))

  val sample = cleanLOgLines.sample(withReplacement = true, fraction = 0.1)
  sample.saveAsTextFile("out/sample_nasa_logs.csv")


  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}
