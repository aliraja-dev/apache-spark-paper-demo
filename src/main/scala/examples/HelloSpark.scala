package examples

import java.util.Properties

import scala.language.implicitConversions
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object HelloSpark extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if(args.length==0){
      logger.error("Please provide filename")
      System.exit(1)
    }

val spark = SparkSession.builder().appName(" Hello Spark")
    .master("local[3]").getOrCreate()

    val surveyRawDF = loadSurveyDF(spark, "data/sample.csv")

    val partitionedSurveyDF = surveyRawDF.repartition(2)
    val countDF = countByCountry(partitionedSurveyDF)
    countDF.foreach(row => {
      logger.info("Country: " + row.getString(0) + " Count: " + row.getLong(1))
    })

   logger.info(countDF.collect().mkString("->"))

    //countDF.show()
    logger.info("Starting Hello Spark")
    logger.info("Spark.conf="+ spark.conf.getAll.toString())


    logger.info("Finished Hello Spark")
    //to hold the spark session, during deve
    scala.io.StdIn.readLine()
    spark.stop()
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }


  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    //This is a fix for Scala 2.11
   // import scala.collection.JavaConverters._
   // props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
    sparkAppConf
  }
}
