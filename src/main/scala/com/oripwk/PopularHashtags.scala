package com.oripwk

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import scala.concurrent.duration._

object PopularHashtags {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val stream = TwitterUtils.createStream(ssc, None)
    stream
      .flatMap(_.getHashtagEntities)
      .map(_.getText)
      .map(_ -> 1)
      .reduceByKeyAndWindow((_: Int) + (_: Int), Minutes(1), Seconds(1))
      .transform(_.sortBy(a => a._2, ascending = false))
      .print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(5.minutes.toMillis)
  }

}
