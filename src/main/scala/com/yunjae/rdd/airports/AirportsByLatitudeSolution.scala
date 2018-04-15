package com.yunjae.rdd.airports

import com.yunjae.commons.Utils
import com.yunjae.rdd.airports.AirportsInUsaSolution.{airportsInUSA, airportsNameAndCityNames}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeSolution extends App {
  val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val airports = sc.textFile("in/airports.text")
  val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 30)

  val airportsNameAndCityNames = airportsInUSA.map(line => {
    val splits = line.split(Utils.COMMA_DELIMITER)
    splits(1) + ", " + splits(6) // 위도
  })

  airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")
}
