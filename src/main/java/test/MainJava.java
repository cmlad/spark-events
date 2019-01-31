package test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class MainJava {
    public static void main(String[] args) throws StreamingQueryException {

        // Start events emitter server in a separate thread
        new EventsEmitterServerJava().start();

        // Spark session
        SparkSession spark = SparkSession.builder()
            .appName("Test")
            .master("local[4]")
            .getOrCreate();

        // Json schema
        StructType jsonSchema = new StructType()
            .add("user_id", DataTypes.StringType)
            .add("timestamp", DataTypes.LongType)
            .add("type", DataTypes.StringType)
            .add("btc", DataTypes.createDecimalType(20, 10))
            .add("payment_method", DataTypes.StringType);

        // Connect to emitter server and start reading
        Dataset<Row> inputDf = spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
            // Parse JSON and replace columns
            .withColumn(
                "json_data",
                functions.from_json(functions.col("value"), jsonSchema)
            )
            .select("json_data.*");

        // Process events
        Dataset<Row> outputDf = processEvents(inputDf);

        // Write output to console
        StreamingQuery query = outputDf.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }

    private static Dataset<Row> processEvents(Dataset<Row> inputDf) {
        //
        // Write events aggregation code in this method
        //

        return inputDf;
    }
}
