package com.yunjae.pairRdd.groupbyKey

import com.yunjae.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountrySolution extends App {

  val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val airports = sc.textFile("in/airports.text")
  val airportsByCountry = airports
    .map(line => (line.split(Utils.COMMA_DELIMITER)(3), line.split(Utils.COMMA_DELIMITER)(2)))
    .groupByKey()

  for ((country, airportName) <- airportsByCountry.collectAsMap()) println(s"$country : ${airportName.toList}")
}
