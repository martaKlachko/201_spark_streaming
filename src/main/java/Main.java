import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;


public class Main {

    public static void main(String[] args) {

        String path = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output";

        SparkConf conf = new SparkConf().setAppName("201_streaming_spark");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaDStream<String> data = jssc.textFileStream(path);
        System.out.println(data.count());


    }
}
