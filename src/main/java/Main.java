import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class Main {

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {

      //  String path_2016 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2016";
        String path_2016 = args[0];
        //"C:\\Users\\Marta_Kurman\\201_streaming_spark\\src\\main\\resources\\201_expedia_output\\ci_year=2016";
      //  String path_2017 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2017";
        String path_2017 =args[1];
        //"C:\\Users\\Marta_Kurman\\201_streaming_spark\\src\\main\\resources\\201_expedia_output\\ci_year=2017";
       // String hotels_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels";
      //  String hotels_weather_joined_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels_weather_joined";
        String hotels_weather_joined_path = args[2];
        //"C:\\Users\\Marta_Kurman\\201_streaming_spark\\src\\main\\resources\\hotels_weather_joined";
      //  String weather_path = "hdfs://sandbox-hdp.hortonworks.com:8020/weather";

//
//        // The schema is encoded in a string
//        String schemaString = "id hotel_id srch_ci srch_co lag_day diff value";


        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .master("local[2]")
                .config("spark.sql.streaming.schemaInference", true)
                .config("spark.local.dir", "/tmp/spark-temp")
                .getOrCreate();


        Dataset<Row> data_2016 = spark
                .readStream()
                .format("parquet")
                .parquet(path_2016);

        Dataset<Row> data_2017 = spark
                .readStream()
                .format("parquet")
                .parquet(path_2017);



        Dataset<Row> hotels_weather_joined = spark
                .readStream()
                .format("csv")
                .option("header", "false")
                .option("inferSchema", "true")
                .csv(hotels_weather_joined_path);



//        Dataset<Row> data_2016_with_watermark = data_2016.withWatermark("lag_day", "2 hours");
//        Dataset<Row> data_2017_with_watermark = data_2017.withWatermark("lag_day", "2 hours");
        //    Dataset<Row> hotels_weather_joined_with_watermark = hotels_weather_joined.withWatermark("_c13", "2 hours");
        Dataset<Row> data  =  data_2016.union(data_2017);


        Dataset<Row> data_joined =  data.select("id","hotel_id", "srch_ci", "srch_co").as("d").join(hotels_weather_joined.as("h")) // INNER JOIN is the default
                .where("d.hotel_id = h._c0");
        data_joined.createOrReplaceTempView("data_joined");

        Dataset<Row> data_joined_selected = spark.sql("select id, hotel_id, " +
                "srch_ci, srch_co, _c7 as lat, _c8 as lng, _c11 as avg_tmpr_f , _c12 as avg_tmpr_c , _c13 as wthr_date from data_joined ");

        Dataset<Row> data_joined_filtered = data_joined_selected.filter(data_joined_selected.col("avg_tmpr_f").$greater(0)
                .or(data_joined_selected.col("avg_tmpr_c").$greater(0)));

//
//        Dataset<Row> data_joined_duration =data_joined.withColumn("duration", data_joined.col("srch_co")
//                        .$minus(data_joined.col("srch_ci")));
        data_joined_filtered.coalesce(1).writeStream()
                .format("parquet")
                .outputMode(OutputMode.Append())
                .option("checkpointLocation", "/checkpoint3")
                .start("gs://spark_str/output")
                .awaitTermination();

    }
}
