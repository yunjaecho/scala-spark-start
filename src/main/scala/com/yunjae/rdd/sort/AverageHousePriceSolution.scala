package com.yunjae.rdd.sort

import com.yunjae.pairRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution extends App {
  val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/RealEstate.csv")
  val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))
  val housepPricePairRdd = cleanedLines.map(line => (line.split(",")(3).toInt, AvgCount(1, line.split(",")(2).toDouble)))

  val hosePriceTotal = housepPricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

  val hosePriceAvg = hosePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)

  val sortedHousePriceAvg = hosePriceAvg.sortByKey(ascending = false)

  for((bedrooms, avgPrice) <- sortedHousePriceAvg.collect()) println(bedrooms + " : " + avgPrice)
}
