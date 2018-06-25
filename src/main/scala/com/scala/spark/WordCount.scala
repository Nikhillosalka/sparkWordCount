package com.scala.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Nikhil on 24/06/18.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.
      builder().
      appName("example-spark-scala-read-and-write-from-hdfs").
      getOrCreate()
    val hdfs_master = args{0}
    val inputFile = hdfs_master + args{1}
    val outputFile = hdfs_master + args{2}
    val sc = sparkSession.sparkContext
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile(outputFile)
  }
}
