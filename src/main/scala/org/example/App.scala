package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaSpark")
      .master("local")
      .getOrCreate()


    val directoryPath = "src/main/scala/org/example/Sessions/*"
    val readFile = spark.sparkContext.wholeTextFiles(directoryPath).map(_._2)

    countDocumentACC45616(readFile)


    spark.stop()
  }

  def countDocumentACC45616(filePath: RDD[String]) : Unit = {

    val blockPattern = """(?s)CARD_SEARCH_START(.*?)CARD_SEARCH_END""".r
    val regexPattern = ".*ACC_45616.*".r

    val blocks = filePath.flatMap { fileContent =>
      blockPattern.findAllMatchIn(fileContent).map(_.group(1)) // Извлекаем содержимое блоков
    }

    val matchingBlocks = blocks.filter(block => regexPattern.findFirstIn(block).isDefined)

    val count = matchingBlocks.count()
    println(s"Количество поисков документа с идентификатором ACC_45616: $count")
  }

  def countDocumentsQS (filePath: RDD[String]) : Unit = {

    

  }

}

