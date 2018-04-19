package com.yunjae.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HousePriceSolution extends App {

  val PRICE_SQ_FT = "Price SQ Ft"

  Logger.getLogger("org").setLevel(Level.ERROR)
  // SparkSql SparkSession used
  val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

  val realEstate = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/RealEstate.csv")

  realEstate.printSchema()

  realEstate.groupBy("Location")
    .avg(PRICE_SQ_FT)
    .orderBy("avg(Price SQ Ft)")
    .show()


}
