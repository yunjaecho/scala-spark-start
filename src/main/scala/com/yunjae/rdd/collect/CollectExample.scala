package com.yunjae.rdd.collect

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CollectExample extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("collect").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
  val wordRdd = sc.parallelize(inputWords)

  val words = wordRdd.collect()

  for (word <- words) println(word)
}
