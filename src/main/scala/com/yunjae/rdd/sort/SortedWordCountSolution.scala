package com.yunjae.rdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SortedWordCountSolution extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/word_count.text")
  val wordRdd = lines.flatMap(_.split(" "))

  val wordToCountPairRdd = wordRdd
    .map((_, 1))
    .reduceByKey(_ + _)
    .map(wordToCount => (wordToCount._2, wordToCount._1))

  //wordToCountPairRdd.foreach(println(_))

  val sortedCountToWordPairs = wordToCountPairRdd.sortByKey(ascending = false)

  val sortedWordToCountPairs = sortedCountToWordPairs.map(countToWord => (countToWord._2, countToWord._1))

  for((word, count) <- sortedWordToCountPairs.collect()) println(word + " : " + count)



}
