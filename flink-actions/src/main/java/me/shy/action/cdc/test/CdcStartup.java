package me.shy.action.cdc.test;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.oracle.source.OraclePooledDataSourceFactory;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder.OracleIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import com.ververica.cdc.connectors.oracle.source.utils.OracleSchema;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;

public class CdcStartup {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf",
                "C:\\Users\\shy\\Documents\\applications\\kerberos-4.1\\krb5.ini");
        System.setProperty("hadoop.home.dir",
                "C:\\Users\\shy\\Documents\\applications\\winutils-master\\Hadoop-2.10.0");
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("hadoop.rpc.protection", "Privacy");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("shy/testbased@SHY.ME",
                "C:\\Users\\shy\\Downloads\\hadoop.keytab");

        List<String> tableNames = new ArrayList<>();
        tableNames.add("STUDENTS");
        tableNames.add("CLASS");

        String username = "flinkuser";
        String password = "123456";
        String url = String.format("jdbc:oracle:thin:@%s:%s:%s", "testbased", 1521, "ORCL");

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.tablename.case.insensitive", "true");
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");
        debeziumProperties.setProperty("log.mining.continuous.mine", "true");
        debeziumProperties.setProperty("json.schema-include", "true");
        debeziumProperties.setProperty("include.schema.comments", "true");

        JdbcIncrementalSource<JsonRecord> oracleChangeEventSource = new OracleSourceBuilder<JsonRecord>()
                .url(url)
                .databaseList("ORCL")
                .schemaList("FLINKUSER")
                .tableList("FLINKUSER.STUDENTS", "FLINKUSER.CLASS")
                .username(username)
                .password(password)
                .deserializer(new CustomDebeziumDeserializationSchema(false))
                .includeSchemaChanges(false) // output the schema changes as well
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .splitSize(2)
                .build();

        // Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);
        DataStreamSource<JsonRecord> oracleParallelSource = env.fromSource(oracleChangeEventSource,
                WatermarkStrategy.noWatermarks(), "OracleParallelSource");

        Connection connection = DriverManager.getConnection(url, username, password);
        Map<String, OutputTag<JsonRecord>> outputTagMap = new HashMap<>();
        Map<String, List<FieldInfo>> tableInfos = new HashMap<>();

        for (String tableName : tableNames) {
            outputTagMap.put(tableName,
                    new OutputTag<>(tableName, TypeInformation.of(JsonRecord.class)));

            // Orig schema
            List<FieldInfo> fieldInfos = new ArrayList<>();
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getColumns("ORCL", "FLINKUSER", tableName, null)) {
                while (rs.next()) {
                    String fieldName = rs.getString("COLUMN_NAME");
                    String fieldType = rs.getString("TYPE_NAME");
                    Integer precision = rs.getInt("COLUMN_SIZE");
                    String fieldComment = rs.getString("REMARKS");
                    if (rs.wasNull()) {
                        precision = null;
                    }
                    Integer scale = rs.getInt("DECIMAL_DIGITS");
                    if (rs.wasNull()) {
                        scale = null;
                    }
                    FieldInfo fieldInfo = new FieldInfo(fieldName, fieldType, fieldComment, precision, scale);
                    fieldInfos.add(fieldInfo);
                }
            }
            tableInfos.put(tableName, fieldInfos);

        }

        connection.close();

        // Create Iceberg Table, using Hadoop Catalog
        Map<String, String> hiveCatalogProperties = new HashMap<>();
        hiveCatalogProperties.put(CatalogProperties.URI, "thrift://testbased:9083");
        hiveCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "hdfs://testbased/data/hive");
        hiveCatalogProperties.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
        CatalogLoader icebergClog = CatalogLoader.hive(
                "iceberg_clog", configuration, hiveCatalogProperties);

        TableIdentifier students = TableIdentifier.of("demo", "students");
        Set<Integer> ids = new HashSet<>();
        ids.add(1);
        List<NestedField> stuColumns = new ArrayList<>();
        stuColumns.add(NestedField.required(1, "id", IntegerType.get()));
        stuColumns.add(NestedField.required(2, "name", StringType.get()));
        stuColumns.add(NestedField.required(3, "age", IntegerType.get()));
        Schema studentSchema = new Schema(stuColumns, ids);
        
        if(!icebergClog.loadCatalog().tableExists(students)) {
            icebergClog.loadCatalog()
                    .createTable(students, 
                            studentSchema, 
                            PartitionSpec.unpartitioned(), 
                            Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
        }
        
        TableIdentifier classInfo = TableIdentifier.of("demo", "class");
        List<NestedField> classColumns = new ArrayList<>();
        classColumns.add(NestedField.required(1, "id", IntegerType.get()));
        classColumns.add(NestedField.required(2, "class_name", StringType.get()));
        Schema classSchema = new Schema(classColumns, ids);

        if (!icebergClog.loadCatalog().tableExists(classInfo)) {
            icebergClog.loadCatalog()
                    .createTable(classInfo, 
                            classSchema, 
                            PartitionSpec.unpartitioned(), 
                            Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
        }
        

        // process
        SingleOutputStreamOperator<JsonRecord> mainStream = oracleParallelSource.process(
                new ProcessFunction<JsonRecord, JsonRecord>() {
                    @Override
                    public void processElement(JsonRecord value, Context ctx, Collector<JsonRecord> out) {
                        int index = value.getTableName().lastIndexOf(".");
                        String originalName = value.getTableName().substring(index + 1);
                        ctx.output(outputTagMap.get(originalName), value);
                    }
                });

        for (String tableName : tableNames) {
            SideOutputDataStream<JsonRecord> outputStream =
                    mainStream.getSideOutput(outputTagMap.get(tableName));

            // CustomSinkFunction sinkFunction = new CustomSinkFunction ();//自定义sink
            // outputStream.addSink(sinkFunction).name(tableName);
            // outputStream.print(String.format("Table ===> %s", tableName));

            DataStream<RowData> rowData = outputStream
                    .map((MapFunction<JsonRecord, RowData>) (value) -> {
                        ObjectMapper map = new ObjectMapper();
                        Map<String, Object> rowMap = map.readValue(
                                value.getFieldValues(),
                                new TypeReference<Map<String, Object>>() {
                                });
                        GenericRowData row = new GenericRowData(rowMap.keySet().size());
                        List<FieldInfo> fieldInfos = tableInfos.get(tableName);
                        for (int i = 0; i < fieldInfos.size(); i++) {

                            String name = fieldInfos.get(i).getName();
                            String type = fieldInfos.get(i).getType().toUpperCase();
                            Integer precision = fieldInfos.get(i).getPrecision();
                            if(rowMap.get(name) == null ) {
                                continue;
                            }
                            switch (type) {
                                case "NUMBER":
                                    row.setField(i, rowMap.get(name));
                                    break;
                                case "VARCHAR2":
                                case "VARCHAR":
                                    row.setField(i,
                                            StringData.fromString((String) rowMap.get(name)));
                                    break;
                                default:
                                    throw new UnsupportedOperationException(
                                            String.format("Unsupport data type: %s in table %s", type, tableName));
                            }
                        }
                        row.setRowKind(RowKind.valueOf(value.getOp()));
                        return row;
                    });

            // Iceberg sink
            
            FlinkSink.forRowData(rowData)
                    .tableLoader(TableLoader.fromCatalog(icebergClog, TableIdentifier.of("demo", tableName)))
                    .equalityFieldColumns(Arrays.asList("id"))
                    .upsert(true)
                    .append();
        }
        env.execute();
    }

}
