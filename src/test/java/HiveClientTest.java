import junit.framework.TestCase;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HiveClientTest {


    @Test
    public void TestTableCreation() throws Exception {

        SparkSession sparkSession = SparkUtils.getSparkSession();
        Dataset<Row> csv = sparkSession.read().option("header", "true").option("delimiter", ";").csv("src/test/resources/people.csv");

        HiveClient hiveClient = new HiveClient();
        IMetaStoreClient metaStoreClient = hiveClient.createMetaStoreClient("src/test/resources/hive-site.xml");
        List<String> keys = new ArrayList<>();
        keys.add("job");
        hiveClient.prepareTable("default", "t1", metaStoreClient,
                csv.schema(), keys, "/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse/t1");

        csv.write().partitionBy(keys.get(0)).mode(SaveMode.Append).
                parquet("/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse/t1");

        sparkSession.sql(" ALTER TABLE default.t1 ADD PARTITION (job='Developer') " +
                "LOCATION '/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse/t1/job=Developer' ");


        csv.select(csv.col(keys.get(0))).distinct().collectAsList();

        String location = metaStoreClient.getTable("default", "t1").getSd().getLocation();
        //make the path and add the partition

        sparkSession.sql("select * from default.t1 ").show(100, false);

        System.out.println("tetsou");


    }


}