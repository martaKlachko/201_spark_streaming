import org.apache.spark.sql.SparkSession;

public class SparkConfig {
    static SparkSession getSession() {
        return  SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .master("yarn")
                .config("spark.sql.streaming.schemaInference", true)
                .config("spark.local.dir", "/tmp/spark-temp")
                .getOrCreate();
    }
}
