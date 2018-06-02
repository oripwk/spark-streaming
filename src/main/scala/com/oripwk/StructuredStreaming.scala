package com.oripwk

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StructuredStreaming {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()

    val stream = spark.readStream.text("logs")

    import spark.implicits._

    val dataSet = stream
      .flatMap(parseLog)
      .select("status", "dateTime")
      .groupBy($"status", window($"dateTime", "1 hour"))
      .count()
      .orderBy("window")

    val query = dataSet.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

    spark.stop()
  }

  case class LogEntry(
    ip: String,
    client: String,
    user: String,
    dateTime: String,
    request: String,
    status: String,
    bytes: String,
    referer: String,
    agent: String
  )

  def parseDateField(field: String): Option[String] = {
    val datePattern = Pattern.compile("\\[(.*?) .+]")
    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = dateFormat.parse(dateString)
      val timestamp = new java.sql.Timestamp(date.getTime)
      Option(timestamp.toString)
    } else {
      None
    }
  }

  def parseLog(x: Row): Option[LogEntry] = {
    val matcher = apacheLogPattern().matcher(x.getString(0))

    if (matcher.matches()) Some(LogEntry(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      parseDateField(matcher.group(4)).getOrElse(""),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9)
    )) else None
  }

}