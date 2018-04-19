package com.yunjae.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StackOverFlowSurvey {
  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // SparkSql SparkSession used
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    println("==============  Print out schema =================")
    responses.printSchema()

    val reponseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)
    println("==============  Print the selected columns of the table  =================")
    // Sampling (20 rows)
    reponseWithSelectedColumns.show()

    println("==============  Print records where the response is from Afghanistan  =================")
    reponseWithSelectedColumns.filter(reponseWithSelectedColumns.col("country") === "Afghanistan").show()

    println("==============  Print the count of occupations  =================")
    val groupedDataset = reponseWithSelectedColumns.groupBy("occupation")
    groupedDataset.count().show()

    println("==============  Print records with average mid age less than 20  =================")
    reponseWithSelectedColumns.filter(reponseWithSelectedColumns.col(AGE_MIDPOINT) < (20)).show()

    println("==============  Print the result by salary middle point in descending order  =================")
    reponseWithSelectedColumns.orderBy(reponseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

    println("==============  Group by country and aggregate by average salary middle point and max age middle point  =================")
    val datasetGroupbyCountry = reponseWithSelectedColumns.groupBy("country")
    datasetGroupbyCountry.avg(SALARY_MIDPOINT).show()

    val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET, responses.col(SALARY_MIDPOINT).divide(20000).cast("Integer").multiply(20000))

    println("============== with salary bucket column =================")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    println("============== Group by salary bucket =================")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
  }
}
