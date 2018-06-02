package com.oripwk

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("word-count")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    val result = sc.textFile("/Users/orip/Downloads/book.txt")
        .flatMap(line => line.split(' '))
        .map(_.toLowerCase)
        .countByValue()

    println(result.take(10).toMap)
    sc.stop()
  }

}
