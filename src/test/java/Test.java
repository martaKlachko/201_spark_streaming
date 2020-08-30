import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public class Test {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession
                .builder()
                .appName("JavaStructuredStreaming")
                .master("yarn")
                .config("spark.sql.streaming.schemaInference", true)
                .config("spark.local.dir", "/tmp/spark-temp")
                .getOrCreate();
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

    @org.junit.Test
    public void readCSV() {
        Dataset<Row> df = TaskUtils.readCSV(spark, "src/test/resources/1.csv");
        Assert.assertNotNull(df);
    }

    @org.junit.Test
    public void readParquet() {
        Dataset<Row> df = TaskUtils.readParquet(spark, "src/test/resources/2.snappy.parquet");
        Assert.assertNotNull(df);
    }

    @org.junit.Test
    public void testWindowData() {

        Dataset<Row> df_1 = TaskUtils.readParquet(spark, "src/test/resources/1.snappy.parquet");
        Dataset<Row> df_2 = TaskUtils.readParquet(spark, "src/test/resources/2.snappy.parquet");
        Dataset<Row> df_3 = TaskUtils.readCSV(spark, "src/test/resources/1.csv");
        Row[] actual = TaskUtils.task1(spark, df_1, df_2, df_3).collect();

        Dataset<Row> data = df_1.union(df_2);
        Dataset<Row> data_joined = data.select("id", "hotel_id", "srch_ci", "srch_co").as("d").join(df_3.as("h")) // INNER JOIN is the default
                .where("d.hotel_id = h._c0");
        data_joined.createOrReplaceTempView("data_joined");

        Dataset<Row> data_joined_renamed = spark.sql("select id, hotel_id, " +
                "TO_DATE(CAST(UNIX_TIMESTAMP(srch_ci, 'yyyy-mm-dd') AS TIMESTAMP)) as srch_ci," +
                " TO_DATE(CAST(UNIX_TIMESTAMP(srch_co, 'yyyy-mm-dd') AS TIMESTAMP)) as srch_co, _c7 as lat, _c8 as lng, _c11 as avg_tmpr_f , _c12 as avg_tmpr_c , _c13 as wthr_date from data_joined ");

        Dataset<Row> data_joined_filtered = data_joined_renamed.filter(data_joined_renamed.col("avg_tmpr_f").$greater(0)
                .or(data_joined_renamed.col("avg_tmpr_c").$greater(0)));
        data_joined_filtered.createOrReplaceTempView("data_joined_filtered");

        Dataset<Row> data_with_datediff = spark.sql("SELECT *,DATEDIFF( srch_co, srch_ci ) AS diff_days  from data_joined_filtered");

        Dataset<Row> expected = data_with_datediff
                .withColumn("stay_type", functions.callUDF("sampleUDF", data_with_datediff.col("diff_days")));


        Assert.assertEquals(actual, expected);
    }


}
