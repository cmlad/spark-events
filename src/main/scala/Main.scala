import java.io.{InputStream, PrintStream}
import java.net.ServerSocket
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    // Start events emitter server in a separate thread
    new EventsEmitterServer().start()

    // Spark session
    val spark = SparkSession.builder
      .appName("Test")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Json schema
    val jsonSchema = new StructType()
      .add("user_id", StringType)
      .add("timestamp", TimestampType)
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

    ///////////////////////////////////
    // Write aggregation code
    ///////////////////////////////////






    ///////////////////////////////////

    // Write output to console
    val query = inputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

class EventsEmitterServer extends Thread {
  override def run() {
    println("Events emitter server thread started")

    val stream: InputStream = getClass.getResourceAsStream("/events.jsonl")
    val lines = scala.io.Source.fromInputStream(stream).getLines

    val server = new ServerSocket(9999)

    while (true) {
      val s = server.accept()
      val out = new PrintStream(s.getOutputStream)

      lines.foreach { line =>
        out.println(line.trim)
        Thread.sleep(1000)
        out.flush()
      }

      s.close()
    }
  }
}