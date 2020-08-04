import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Main {

    public static void main(String[] args) throws InterruptedException {

        String path_2016 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2016";
        String path_2017 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2017";
        String hotels_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels";
        String hotels_weather_joined_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels_weather_joined";
        String weather_path = "hdfs://sandbox-hdp.hortonworks.com:8020/weather";

//        SparkConf conf = new SparkConf().setAppName("201_streaming_spark");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .config("spark.sql.streaming.schemaInference", true)
                .getOrCreate();


        Dataset<Row> data_2016 = spark
                .readStream()
                .format("parquet")
                .parquet(path_2016);

        Dataset<Row> data_2017 = spark
                .readStream()
                .format("parquet")
                .parquet(path_2017);

        Dataset<Row> hotels= spark
                .readStream()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(hotels_path);


        Dataset<Row> weather = spark
                .readStream()
                .format("parquet")
                .parquet(weather_path);


//        Dataset<Row> weather_rounded = weather.withColumn("lat_rounded", functions.round(weather.col("lat"), 2))
//                .withColumn("lng_rounded", functions.round(weather.col("lng"), 2));
//        Dataset<Row> hotels_rounded = hotels.withColumn("Latitude_rounded", functions.round(hotels.col("Latitude"), 2))
//                .withColumn("Longitude_rounded", functions.round(hotels.col("Longitude"), 2));
        Dataset<Row> hotels_weather_joined = spark
                .readStream()
                .format("csv")
                .option("header", "false")
                .option("inferSchema", "true")
                .csv(hotels_weather_joined_path);
//
//        System.out.println(hotels_weather_joined.isStreaming());    // Returns True for DataFrames that have streaming sources
//
        //       hotels_weather_joined.printSchema();
        //  hotels.printSchema();

//        Dataset<Row> data_2016_with_watermark = data_2016.withWatermark("lag_day", "2 hours");
//        Dataset<Row> data_2017_with_watermark = data_2017.withWatermark("lag_day", "2 hours");
        Dataset<Row> hotels_weather_joined_with_watermark = hotels_weather_joined.withWatermark("_c13", "2 hours");


        Dataset<Row> joined =  data_2016.as("c").join(hotels_weather_joined.as("i")) // INNER JOIN is the default
                .where("c.hotel_id = i._c0");
        Dataset<Row> union = data_2017.as("c").join(hotels_weather_joined.as("i")) // INNER JOIN is the default
                .where("c.hotel_id = i._c0").union(joined);

        System.out.println(union.count());
    }
}
