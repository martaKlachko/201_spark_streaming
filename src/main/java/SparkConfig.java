import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

public class SparkConfig {
    static SparkSession getSession() {
        return SparkSession.builder()
               // .config("spark.jars", "/home/maria_dev/201_spark_batching/target/201_project_batching-1.0-SNAPSHOT.jar")
               // .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "username")
               // .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "password")
                .config(ConfigurationOptions.ES_NODES, "7442e3172a8e4d3784e5b2acc8a7edac.europe-west3.gcp.cloud.es.io")
                .config(ConfigurationOptions.ES_PORT, "9243")
                .enableHiveSupport().getOrCreate();
    }
}
