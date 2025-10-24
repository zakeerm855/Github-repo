package SparkExample.Interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object wordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local[*]").appName("wordcount").getOrCreate()

    val rdd = spark.sparkContext.textFile("E:\\Intellji\\LearningSparkScala\\src\\main\\resources\\sparkResources\\data\\textFiles\\data.txt")
    rdd.foreach(println)

    val splitData = rdd.flatMap(f => f.split(" "))
    splitData.foreach(println)

    val mapData = splitData.map(word => (word,1))
    mapData.foreach(println)

    val reduceData = mapData.reduceByKey(_ + _)
    reduceData.foreach(println)
  }
}
