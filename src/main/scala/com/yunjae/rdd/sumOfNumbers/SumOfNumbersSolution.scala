package com.yunjae.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersSolution extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/prime_nums.text")

  val numbers = lines.flatMap(line => line.split("\\s+"))

  val validNumbers = numbers
    .filter(number => !number.isEmpty)
    .map(number => number.toInt);

  val sumNumber = validNumbers.reduce((x, y) => x + y)

  println(s"sumNumber : $sumNumber")

}
