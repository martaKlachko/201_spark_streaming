import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;


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

        spark
                .sqlContext()
                .udf()
                .register("sampleUDF", sampleUdf(), DataTypes.StringType);


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
                "TO_DATE(CAST(UNIX_TIMESTAMP(srch_ci, 'yyyy-mm-dd') AS TIMESTAMP)) as srch_ci," +
                " TO_DATE(CAST(UNIX_TIMESTAMP(srch_co, 'yyyy-mm-dd') AS TIMESTAMP)) as srch_co, _c7 as lat, _c8 as lng, _c11 as avg_tmpr_f , _c12 as avg_tmpr_c , _c13 as wthr_date from data_joined ");

        Dataset<Row> data_joined_filtered = data_joined_selected.filter(data_joined_selected.col("avg_tmpr_f").$greater(0)
                .or(data_joined_selected.col("avg_tmpr_c").$greater(0)));
        data_joined_filtered.createOrReplaceTempView("data_joined_filtered");
//
//        Dataset<Row> data_joined_duration =data_joined.withColumn("duration", data_joined.col("srch_co")
//                        .$minus(data_joined.col("srch_ci")));

        Dataset<Row> data_joined_duration = spark.sql("SELECT *,DATEDIFF( srch_co, srch_ci ) AS diff_days  from data_joined_filtered");

        Dataset<Row> data_joined_duration_1 = data_joined_duration
                .withColumn("stay_type",
                        functions.callUDF("sampleUDF", data_joined_duration.col("diff_days")))
                .withColumn("timestamp", functions.current_timestamp());

//        Dataset<Row> data_joined_duration_2 = data_joined_duration_1
//                .withWatermark("timestamp", "20000 milliseconds")
//                .groupBy(
//                        functions.window(data_joined_duration_1.col("timestamp"), "1 minute", "30 seconds"),
//                        data_joined_duration_1.col("hotel_id"), data_joined_duration_1.col("stay_type")).count();
        Dataset<Row> data_joined_duration_2 = data_joined_duration_1
                .withWatermark("timestamp", "1 minute")
                .groupBy(
                        functions.window(functions.column("timestamp"), "1 minute", "30 seconds"),
                        data_joined_duration_1.col("hotel_id"), data_joined_duration_1.col("stay_type"))
                .count();

        data_joined_duration_2.coalesce(1).writeStream()
                .format("parquet")
                .outputMode(OutputMode.Append())

                .option("checkpointLocation", "/checkpoint19")
                .start("gs://spark_str/output")
                .awaitTermination();

    }

    static UDF1<Integer, String> sampleUdf() {
        return (s1) -> {
            if (s1 == 1) {
                return "Short_stay";
            } else if (s1 >= 2 && s1 <= 7) {
                return "Standart_stay";
            } else if (s1 > 7 && s1 <= 14) {
                return "Standart_extended stay";
            } else if (s1 > 14 && s1 <= 30) {
                return "Long_stay";
            } else {
                return "Erroneous_data";
            }
        };
    }
}
