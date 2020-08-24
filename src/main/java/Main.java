import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class Main {

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {

        String path_2016 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2016";
        String path_2017 = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output/ci_year=2017";
        String hotels_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels";
        String hotels_weather_joined_path = "hdfs://sandbox-hdp.hortonworks.com:8020/hotels_weather_joined";
        String weather_path = "hdfs://sandbox-hdp.hortonworks.com:8020/weather";


        // The schema is encoded in a string
        String schemaString = "id hotel_id srch_ci srch_co lag_day diff value";

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();

        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        fields.add(id);
        StructField hotel_id = DataTypes.createStructField("hotel_id", DataTypes.IntegerType, true);
        fields.add(hotel_id);
        StructField srch_ci = DataTypes.createStructField("srch_ci", DataTypes.StringType, true);
        fields.add(srch_ci);
        StructField srch_co = DataTypes.createStructField("srch_co", DataTypes.StringType, true);
        fields.add(srch_co);
        StructField lag_day = DataTypes.createStructField("lag_day", DataTypes.DateType, true);
        fields.add(lag_day);
        StructField diff = DataTypes.createStructField("diff", DataTypes.IntegerType, true);
        fields.add(diff);
        StructField value = DataTypes.createStructField("value", DataTypes.IntegerType, true);
        fields.add(value);

        StructType schema = DataTypes.createStructType(fields);

//        SparkConf conf = new SparkConf().setAppName("201_streaming_spark");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .master("yarn")
                .getOrCreate();


//        Dataset<Row> data_2016 = spark
//                .readStream()
//                .format("parquet")
//
//                .parquet(path_2016);

        Dataset<Row> data_2017 = spark
                .readStream()
                .format("parquet")
                .schema(schema)
                .load(path_2017);

//        Dataset<Row> hotels= spark
//                .readStream()
//                .format("csv")
//                .option("header", "true")
//                .option("inferSchema", "true")
//                .csv(hotels_path);


//        Dataset<Row> weather = spark
//                .readStream()
//                .format("parquet")
//                .parquet(weather_path);
//        Dataset<Row> expedia = data_2017.union(data_2017);
        StreamingQuery query =   data_2017.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();
        query.awaitTermination();

//        Dataset<Row> weather_rounded = weather.withColumn("lat_rounded", functions.round(weather.col("lat"), 2))
//                .withColumn("lng_rounded", functions.round(weather.col("lng"), 2));
//        Dataset<Row> hotels_rounded = hotels.withColumn("Latitude_rounded", functions.round(hotels.col("Latitude"), 2))
//                .withColumn("Longitude_rounded", functions.round(hotels.col("Longitude"), 2));
//        Dataset<Row> hotels_weather_joined = spark
//                .readStream()
//                .format("csv")
//                .option("header", "false")
//                .option("inferSchema", "true")
//                .csv(hotels_weather_joined_path);



//        Dataset<Row> data_2016_with_watermark = data_2016.withWatermark("lag_day", "2 hours");
//        Dataset<Row> data_2017_with_watermark = data_2017.withWatermark("lag_day", "2 hours");
        //    Dataset<Row> hotels_weather_joined_with_watermark = hotels_weather_joined.withWatermark("_c13", "2 hours");


//        Dataset<Row> joined =  expedia.as("e").join(hotels_weather_joined.as("h")) // INNER JOIN is the default
//                .where("e.hotel_id = h._c0");

//        System.out.println(union.count());
//        StreamingQuery query =   joined.writeStream()
//                .format("console")
//                .start();
//        query.awaitTermination();

    }
}
