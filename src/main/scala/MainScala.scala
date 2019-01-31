import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MainScala {
  def main(args: Array[String]): Unit = {

    // Start events emitter server in a separate thread
    new EventsEmitterServerScala().start()

    // Spark session
    val spark = SparkSession.builder
      .appName("Test")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Json schema
    val jsonSchema = new StructType()
      .add("user_id", StringType)
      .add("timestamp", LongType)
      .add("type", StringType)
      .add("btc", DecimalType(20, 10))
      .add("payment_method", StringType)

    // Connect to emitter server and start reading
    val inputDf = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      // Parse JSON and replace columns
      .withColumn("json_data", from_json($"value", jsonSchema))
      .select("json_data.*")

    // Process events
    val outputDf = processEvents(inputDf)

    // Write output to console
    val query = outputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def processEvents(inputDf: DataFrame): DataFrame = {
    //
    // Write events aggregation code in this method
    //

    inputDf
  }
}
