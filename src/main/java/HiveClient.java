import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

@Slf4j
public class HiveClient {

    public IMetaStoreClient createMetaStoreClient(String hiveSitePath) throws MetaException {

        final HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(new Path(hiveSitePath));
        HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
            @Override
            public HiveMetaHook getHook(Table tbl) throws MetaException {
                if (tbl == null) {
                    return null;
                }
                try {
                    HiveStorageHandler storageHandler =
                            HiveUtils.getStorageHandler(hiveConf, tbl.getParameters().get(META_TABLE_STORAGE));
                    return storageHandler == null ? null : storageHandler.getMetaHook();
                } catch (HiveException e) {
                    //log.error(e.toString());
                    throw new MetaException("Failed to get storage handler: " + e);
                }
            }
        };

        return RetryingMetaStoreClient.getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
    }

    public Table createPartitionedTable(
            IMetaStoreClient metaStoreClient,
            String database,
            String table,
            URI location,
            List<FieldSchema> columns,
            List<FieldSchema> partitionKeys,
            String serializationLib,
            String inputFormatClassName,
            String outputFormatClassName)
            throws Exception {

        Table hiveTable = new Table();
        hiveTable.setDbName(database);
        hiveTable.setTableName(table);
        hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
        hiveTable.putToParameters("EXTERNAL", "TRUE");

        hiveTable.setPartitionKeys(partitionKeys);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(columns);
        sd.setLocation(location.toString());
        sd.setParameters(new HashMap<String, String>());
        sd.setInputFormat(inputFormatClassName);
        sd.setOutputFormat(outputFormatClassName);
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setSerializationLib(serializationLib);
        hiveTable.setSd(sd);
        metaStoreClient.createTable(hiveTable);

        return hiveTable;
    }

    public List<FieldSchema> getFieldSchemaList(StructType schema, List<String> keys, List<String> exclude) {
        List<FieldSchema> collect;
        if (Objects.nonNull(keys)) {
            collect = Arrays.stream(schema.fields()).filter(col ->
                    !keys.contains(col.name())
            ).map(col ->
                    new FieldSchema(col.name(), getParquetType(col), "")).collect(Collectors.toList());
        } else {
            collect = Arrays.stream(schema.fields()).filter(col ->
                    exclude.contains(col.name())
            ).map(col ->
                    new FieldSchema(col.name(), getParquetType(col), "")).collect(Collectors.toList());

        }
        return collect;

    }

    private String getParquetType(StructField field) {
        if (field.dataType() == DataTypes.StringType) {
            return "string";
        } else if (field.dataType() == DataTypes.DoubleType) {
            return "double";
        } else if (field.dataType() == DataTypes.IntegerType) {
            return "int";
        } else if (field.dataType() == DataTypes.FloatType) {
            return "float";
        } else if (field.dataType() == DataTypes.TimestampType) {
            return "timestamp";
        } else if (field.dataType() == DataTypes.BooleanType) {
            return "boolean";
        } else if (field.dataType() instanceof DecimalType) {
            return "decimal" + "(" + ((DecimalType) field.dataType()).precision() + "," +
                    ((DecimalType) field.dataType()).scale() + ")";
        } else if (field.dataType() == DataTypes.LongType) {
            return "bigint";
        }
        throw new UnsupportedOperationException(field.name() + " has " + field.dataType() + " which is unsupported");
    }


    public void prepareTable(String dbName, String tableName,IMetaStoreClient metaStoreClient,
                             StructType schema,
                             List<String> keys,
                             String path) throws Exception {

        final List<FieldSchema> columns = getFieldSchemaList(schema, keys, null);
        final List<FieldSchema> partitionKey = getFieldSchemaList(schema, null, keys);

        if (!metaStoreClient.tableExists(dbName, tableName)) {
            createPartitionedTable(metaStoreClient, dbName, tableName,
                    // new URI("/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse"),
                    new URI(path),
                    columns,
                    partitionKey,
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            );
        } else {
            System.out.println(dbName + "." + tableName + " already exist");
        }
    }

    public static void main(String[] args) throws Exception {

        //String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("Java Spark Hive Example")
                //.config("spark.sql.warehouse.dir", "/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse")
                //.config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        HiveClient hiveClient = new HiveClient();
        IMetaStoreClient metaStoreClient = hiveClient.createMetaStoreClient("src/main/resources/hive-site.xml");

        List<FieldSchema> columns = new ArrayList<>();
        columns.add(new FieldSchema("id", "bigint", ""));
        columns.add(new FieldSchema("qty", "bigint", ""));
        columns.add(new FieldSchema("name", "string", ""));

        List<FieldSchema> partitionKey = new ArrayList<>();
        partitionKey.add(new FieldSchema("rx_mth_cd", "string", ""));


        if (!metaStoreClient.tableExists("default", "test1")) {
            hiveClient.createPartitionedTable(metaStoreClient, "default", "test1",
                    new URI("/home/ksingh/github-ksmatharoo/TestHiveClient/spark-warehouse"),
                    columns,
                    partitionKey,
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            );
        }
        spark.sql("show databases ").show(false);
        System.out.println("test");
    }
}