package com.oripwk

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import scala.concurrent.duration._


object PrintTweets {
  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val stream = TwitterUtils.createStream(ssc, None)
    stream
      .map(_.getText)
      .print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(5.minutes.toMillis)
  }
}