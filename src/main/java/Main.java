import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


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

//        Dataset<Row> hotels= spark
//                .readStream()
//                .format("csv")
//                .option("header", "true")
//                .option("inferSchema", "true")
//                .csv(hotels_path);


        Dataset<Row> weather = spark
                .readStream()
                .format("parquet")
                .parquet(weather_path);


        System.out.println(weather.isStreaming());    // Returns True for DataFrames that have streaming sources

        weather.printSchema();
//
//
//        jssc.start();
//        jssc.awaitTermination();
//        System.out.println(data.count());


    }
}
