package examples

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties
import scala.io.Source
import scala.language.implicitConversions

object FlightsSpark extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if(args.length==0){
      logger.error("Please provide filename")
      System.exit(1)
    }

val spark = SparkSession.builder().appName(" Hello Spark").master("local[3]").getOrCreate()

    val surveyRawDF = loadSurveyDF(spark, args(1))
    val partitionedSurveyDF = surveyRawDF.repartition(2)
    val countDF = countByCountry(partitionedSurveyDF)

    countDF.write.format("json").mode(SaveMode.Overwrite)
      .option("path","datasink/flights")
      .save()
    logger.info("Finished Hello Spark")
    //to hold the spark session, during deve
  // scala.io.StdIn.readLine()
    spark.stop()
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF.where("DISTANCE < 500")
      .select("FL_DATE", "OP_CARRIER", "ORIGIN", "DEST")
      .groupBy("ORIGIN")
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
    sparkAppConf
  }
}
