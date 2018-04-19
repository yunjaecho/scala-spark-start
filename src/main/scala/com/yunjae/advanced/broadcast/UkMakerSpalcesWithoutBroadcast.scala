package com.yunjae.advanced.broadcast

import com.yunjae.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object UkMakerSpalcesWithoutBroadcast extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("UkMakerSpalces").setMaster("local[1]")
  val sc = new SparkContext(conf)


  val postCodeMap = localPostCodeMap()

  val makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv")

  val regions = makerSpaceRdd
    .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .map(line => {
        getPostPrefix(line).filter(prefix => postCodeMap.contains(prefix))
          .map(prefix => postCodeMap(prefix))
          .headOption.getOrElse("Unknown")
      })

  for((regin, count) <- regions.countByValue()) println(regin + " : " + count)


  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }


  def localPostCodeMap(): Map[String, String] = {
    Source.fromFile("in/uk-postcode.csv").getLines().map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      splits(0) -> splits(7)
    }).toMap
  }

}
