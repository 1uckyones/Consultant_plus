package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaSpark")
      .master("local[*]")
      .getOrCreate()

    val directoryPath = "src/main/scala/org/example/Sessions/*"
    val readFile = spark.sparkContext.wholeTextFiles(directoryPath).map(_._2)

    processEvents(readFile, spark)
    spark.stop()
  }

  def processEvents(filePath: RDD[String], spark: SparkSession): Unit = {

    val lines = filePath.flatMap(_.split("\n"))

    val eventBlocks = extractEvents(lines)

    val eventDF = createEventDF(eventBlocks, spark)

    findCardSearchACC_45616(eventDF,spark)
    findQSDocuments(eventDF,spark)
  }

  def extractEvents(lines: RDD[String]): RDD[Row] = {
    var currentEvent = ""
    var currentDate = ""
    var currentId = ""
    var currentDocuments = List[String]()
    var cardSearchData = List[String]()
    var isInsideCardSearch = false
    var isInsideQS = false

    lines.flatMap { line =>
      if (line.startsWith("CARD_SEARCH_START")) {
        currentEvent = "CARD_SEARCH"
        currentDate = line.split(" ")(1).split("_")(0)
        isInsideCardSearch = true
        cardSearchData = List(line)
        None
      } else if (line.startsWith("CARD_SEARCH_END")) {
        val row = Row("CARD_SEARCH", currentDate, "", cardSearchData.mkString(" "))
        isInsideCardSearch = false
        cardSearchData = List()
        Some(row)
      } else if (line.startsWith("QS")) {
        currentEvent = "QS"
        currentDate = line.split(" ")(1).split("_")(0)
        isInsideQS = true
        currentId = ""
        currentDocuments = List()
        None
      } else if (isInsideQS && !line.startsWith("{")) {
        val parts = line.split(" ")
        currentId = parts.head
        currentDocuments = parts.tail.toList
        val row = Row("QS", currentDate, currentId, currentDocuments.mkString(" "))
        isInsideQS = false
        Some(row)
      } else if (line.startsWith("DOC_OPEN")) {
        val parts = line.split(" ")
        Some(Row("DOC_OPEN", parts(1).split("_")(0), parts(2), parts(3)))
      } else if (isInsideCardSearch) {
        cardSearchData = cardSearchData :+ line
        None
      } else {
        None
      }
    }
  }

  def createEventDF(rdd: RDD[Row], spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("Event", StringType, nullable = false),
      StructField("Date", StringType, nullable = false),
      StructField("ID", StringType, nullable = true),
      StructField("Documents", StringType, nullable = true)
    ))
    spark.createDataFrame(rdd, schema)
  }

  def findCardSearchACC_45616 (eventDF: DataFrame, spark: SparkSession): Unit = {
    eventDF.createOrReplaceTempView("events")
    val ans = spark.sql("SELECT COUNT(EVENT) from events WHERE Event = 'CARD_SEARCH' and Documents LIKE '%ACC_45616%'")
    ans.show(false)
  }

  def findQSDocuments (eventDF: DataFrame, spark: SparkSession): Unit = {
    eventDF.createOrReplaceTempView("events")
    val ans = spark.sql("SELECT Date, Documents, COUNT(Event) from events WHERE Event = 'DOC_OPEN' and ID IN (SELECT ID from events WHERE Event = 'QS') GROUP BY Date, Documents")
    ans.show( false)
  }
}
