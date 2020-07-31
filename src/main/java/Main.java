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

        String path = "hdfs://sandbox-hdp.hortonworks.com:8020/expedia/";

//        SparkConf conf = new SparkConf().setAppName("201_streaming_spark");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .getOrCreate();



        Dataset<Row> data = spark
                .readStream()
                .format("parquet")
                .parquet(path);


        System.out.println(data.isStreaming());    // Returns True for DataFrames that have streaming sources

        data.printSchema();
//
//
//        jssc.start();
//        jssc.awaitTermination();
//        System.out.println(data.count());


    }
}
