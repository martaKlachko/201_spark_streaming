import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;


public class Main {

    public static void main(String[] args) throws InterruptedException {

        String path_2016 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2016";
        String path_2017 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2017";
        String hotels_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels";
        String weather_path = "hdfs://sandbox-hdp.hortonworks.com:8020/weather";

//        SparkConf conf = new SparkConf().setAppName("201_streaming_spark");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .config("spark.sql.streaming.schemaInference", true)
                .getOrCreate();



//        Dataset<Row> data_2016 = spark
//                .readStream()
//                .format("parquet")
//                .parquet(path_2016);
//
//        Dataset<Row> data_2017 = spark
//                .readStream()
//                .format("parquet")
//                .parquet(path_2017);

        Dataset<Row> hotels= spark
                .readStream()
                .format("csv")
                .csv(hotels_path);


//        Dataset<Row> weather = spark
//                .readStream()
//                .format("parquet")
//                .parquet(weather_path);


        System.out.println(hotels.isStreaming());    // Returns True for DataFrames that have streaming sources

        hotels.printSchema();
//
//
//        jssc.start();
//        jssc.awaitTermination();
//        System.out.println(data.count());


    }
}
