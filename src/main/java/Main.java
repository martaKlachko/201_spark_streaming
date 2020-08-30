import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class Main {

    public static void main(String[] args) throws StreamingQueryException {


        String path_2016 = args[0];
        String path_2017 = args[1];
        String hotels_weather_joined_path = args[2];
        String output_path = "gs://spark_str/output";

        SparkSession spark = SparkConfig.getSession();
        Dataset<Row> data_2016 = TaskUtils.readParquet(spark, path_2016);
        Dataset<Row> data_2017 = TaskUtils.readParquet(spark, path_2017);

        Dataset<Row> hotels_weather_joined = TaskUtils.readCSV(spark, hotels_weather_joined_path);
        TaskUtils.registerUDF(spark, "sampleUDF");

        Dataset<Row> data = TaskUtils.task1(spark, data_2016, data_2017, hotels_weather_joined);

        TaskUtils.writeStream(data, output_path);


    }


}
