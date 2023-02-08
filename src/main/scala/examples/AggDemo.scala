package examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object AggDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/invoices.csv")

    invoiceDF.select(count("*"),
      sum("Quantity"),
      countDistinct("InvoiceNo")).show()

  }

}
