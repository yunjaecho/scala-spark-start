package com.yunjae.pairRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList extends App {

  val conf = new SparkConf().setAppName("create").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val tuple = List(("Lily", 23), ("Jack", 23), ("Mary", 23), ("James", 23))
  val regularRdd = sc.parallelize(tuple)

  regularRdd.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")

}
