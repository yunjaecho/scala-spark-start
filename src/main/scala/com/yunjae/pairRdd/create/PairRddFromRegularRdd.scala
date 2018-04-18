package com.yunjae.pairRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromRegularRdd extends App {
  val conf = new SparkConf().setAppName("create").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val inputStrings = List("Lily 23", "Jack 23", "Mary 23", "James 23")
  val regularRdd = sc.parallelize(inputStrings).map(s => (s.split(" ")(0), s.split(" ")(1)))

  regularRdd.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd")

}
