import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeUnit;

public class TaskUtils {

    static Dataset<Row> readCSV(SparkSession ss, String path) {

        return ss
                .readStream()
                .format("csv")
                .option("header", "false")
                .option("inferSchema", "true")
                .csv(path);
    }

    static Dataset<Row> readParquet(SparkSession ss, String path) {
        return ss
                .readStream()
                .format("parquet")
                .parquet(path);

    }

    static Dataset<Row> task1(SparkSession ss, Dataset<Row> ds1, Dataset<Row> ds2, Dataset<Row> ds3) {
        Dataset<Row> data = ds1.union(ds2);
        Dataset<Row> data_joined = data.select("id", "hotel_id", "srch_ci", "srch_co").as("d").join(ds3.as("h")) // INNER JOIN is the default
                .where("d.hotel_id = h._c0");
        data_joined.createOrReplaceTempView("data_joined");

        Dataset<Row> data_joined_renamed = ss.sql("select id, hotel_id, " +
                "TO_DATE(CAST(UNIX_TIMESTAMP(srch_ci, 'yyyy-mm-dd') AS TIMESTAMP)) as srch_ci," +
                " TO_DATE(CAST(UNIX_TIMESTAMP(srch_co, 'yyyy-mm-dd') AS TIMESTAMP)) as srch_co, _c7 as lat, _c8 as lng, _c11 as avg_tmpr_f , _c12 as avg_tmpr_c , _c13 as wthr_date from data_joined ");

        Dataset<Row> data_joined_filtered = data_joined_renamed.filter(data_joined_renamed.col("avg_tmpr_f").$greater(0)
                .or(data_joined_renamed.col("avg_tmpr_c").$greater(0)));
        data_joined_filtered.createOrReplaceTempView("data_joined_filtered");

        Dataset<Row> data_with_datediff = ss.sql("SELECT *,DATEDIFF( srch_co, srch_ci ) AS diff_days  from data_joined_filtered");

        Dataset<Row> data_with_stay_type= data_with_datediff
                .withColumn("stay_type", functions.callUDF("sampleUDF", data_with_datediff.col("diff_days")));

        return data_with_stay_type;

    }

    static void registerUDF(SparkSession ss, String name) {
        ss.sqlContext()
                .udf()
                .register(name, sampleUdf(), DataTypes.StringType);

    }

    static void writeStream(Dataset<Row> d, String path) throws StreamingQueryException {
        d.coalesce(1)
                .writeStream()
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "/checkpoint031")
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                .start(path)
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
