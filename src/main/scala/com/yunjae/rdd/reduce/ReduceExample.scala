package com.yunjae.rdd.reduce

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReduceExample extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val inputIntegers = 1 to 10
  val integerRdd = sc.parallelize(inputIntegers)

  val product = integerRdd.reduce((x, y) => x * y)
  println(s"product : $product")

}
