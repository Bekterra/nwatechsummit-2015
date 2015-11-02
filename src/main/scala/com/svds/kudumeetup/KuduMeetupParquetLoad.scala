package com.svds.kudumeetup

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object KuduMeetupParquetLoad {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Required arguments: <parquet path> <input file>")
      sys.exit(42)
    }
    val Array(parquetPath, inputFile) = args
    val conf = new SparkConf().setAppName("Load meetup parquet")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val meetup = sqlContext.jsonFile(inputFile)
    meetup.saveAsParquetFile(parquetPath)
  }
}

