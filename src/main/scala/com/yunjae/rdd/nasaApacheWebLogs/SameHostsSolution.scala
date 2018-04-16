package com.yunjae.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object SameHostsSolution extends App {
  val conf = new SparkConf().setAppName("sampeHosts").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val julyFirstLog = sc.textFile("in/nasa_19950701.tsv")
  val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

  val julyFirstHosts = julyFirstLog.map(line => line.split("\t")(0))
  val augustFirstHosts = augustFirstLogs.map(line => line.split("\t")(0))

  val intersection = julyFirstHosts.intersection(augustFirstHosts)

  val cleanedHostIntersection = intersection.filter(host => host != "host")
  cleanedHostIntersection.saveAsTextFile("out/nasa_logs_sample_hosts.csv")

}
