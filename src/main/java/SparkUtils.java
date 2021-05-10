import org.apache.spark.sql.SparkSession;

public class SparkUtils {


    public static SparkSession getSparkSession(){

        //String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("Java Spark Hive Example")
                //.config("spark.sql.warehouse.dir", "/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse")
                //.config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        return spark;
    }
}
