package com.yunjae.pairRdd.filter

import com.yunjae.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaSolution extends App {
  val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val airports = sc.textFile("in/airports.text")
  val airportsInUSA = airports
    .map(line => (line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)))
    .filter(keyValue => keyValue._2 == "\"United States\"")

  airportsInUSA.saveAsTextFile("out/airports_in_usa_pair_rdd.text")
}
