package com.yunjae.rdd.take

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TakeExample extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("take").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
  val wordRdd = sc.parallelize(inputWords)

  val words = wordRdd.take(3)

  words.foreach(println(_))
  //for (word <- words) println(word)

}
