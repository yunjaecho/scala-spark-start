package com.yunjae.advanced.accumulator

import com.yunjae.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object StackOverFlowSurvey extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val total = sc.longAccumulator
  val missingSalaryMidPoint = sc.longAccumulator

  val responseRDD = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

  val responseFromCanada = responseRDD.filter(
    response => {
      val splits = response.split(Utils.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }

      splits(2) == "Canada"
    }
  )

  println("Count of response from Canada : " + responseFromCanada.count())
  println("Total count of response : " + total.value)
  println("Count of response missing salary middle point : " + missingSalaryMidPoint.value)

}
