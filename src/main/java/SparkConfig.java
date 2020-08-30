import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

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
