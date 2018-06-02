package com.oripwk

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.concurrent.duration._


object SaveTweets {
  def main(args: Array[String]): Unit = {
    val context = new StreamingContext("local[*]", "SaveTweets", Seconds(1))

    val stream = TwitterUtils.createStream(context, None)
    var total = 0l
    stream.foreachRDD { (rdd, time) =>
//      val coalesced = rdd.map(_.getText).coalesce(1).cache()
      val coalesced = rdd.map(_.getText)
      total += coalesced.count()
      println(total)
      coalesced.saveAsTextFile(s"tweets_${time.milliseconds}")
      if (total > 1000) {
        stream.stop()
        sys exit 0
      }
    }

    context.start()
    context.awaitTerminationOrTimeout(5.minutes.toMillis)
  }

}
