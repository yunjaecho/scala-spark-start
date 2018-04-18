package com.yunjae.rdd.persist

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PersistExample extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val inputIntegers = 1 to 10
  val integerRdd = sc.parallelize(inputIntegers)

  // storage level
  //
  integerRdd.persist(StorageLevel.MEMORY_ONLY)

  val count = integerRdd.reduce(_ * _)
  println(s"count : $count")
}
