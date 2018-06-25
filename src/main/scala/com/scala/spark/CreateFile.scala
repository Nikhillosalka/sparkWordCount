package com.scala.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Nikhil on 24/06/18.
  */
object CreateFile {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.
      builder().
      appName("example-spark-scala-read-and-write-from-hdfs").
      getOrCreate()

    import sparkSession.implicits._

    case class HelloWorld(message: String)

    val hdfs_master = args{0}
    val seq = Seq("helloworld", " helloworld")
    val df = seq.toDF().coalesce(1)
    df.write.mode(SaveMode.Overwrite).parquet(hdfs_master + args{1})
  }
}
