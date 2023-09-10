package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import me.shy.action.cdc.source.Identifier;
import me.shy.action.cdc.source.JdbcField;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpec.Builder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlActionUtils {
    
    public static final ConfigOption<Boolean> MYSQL_CONVERTER_TINYINT1_BOOL =
            ConfigOptions.key("mysql.converter.tinyint1-to-bool")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Mysql tinyint type will be converted to boolean type by default, if you want to convert to tinyint type, "
                                    + "you can set this option to false.");

    public static Connection getConnection(Configuration mySqlConfig) throws SQLException {
        String url = String.format(
                "jdbc:mysql://%s:%d",
                mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                mySqlConfig.get(MySqlSourceOptions.PORT));
        
        if (mySqlConfig.contains(MYSQL_CONVERTER_TINYINT1_BOOL)) {
            url = String.format(
                    "%s?tinyInt1isBit=%s",
                    url, mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL));
        }
        
        return DriverManager.getConnection(url,
                mySqlConfig.get(MySqlSourceOptions.USERNAME),
                mySqlConfig.get(MySqlSourceOptions.PASSWORD));
    }

    static MySqlSchemasInfo getMySqlTableInfos(
            Configuration mySqlConfig,
            Predicate<String> monitorTablePredication,
            List<Identifier> excludedTables)
            throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        MySqlSchemasInfo mySqlSchemasInfo = new MySqlSchemasInfo();
        try (Connection conn = MySqlActionUtils.getConnection(mySqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (databaseMatcher.matches()) {
                        try (ResultSet tables = metaData.getTables(databaseName, null, "%", null)) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                MySqlSchema mySqlSchema =
                                        MySqlSchema.buildSchema(
                                                metaData,
                                                databaseName,
                                                tableName,
                                                mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL));
                                Identifier identifier = Identifier.create(databaseName, tableName);
                                if (monitorTablePredication.test(tableName)) {
                                    mySqlSchemasInfo.addSchema(identifier, mySqlSchema);
                                } else {
                                    excludedTables.add(identifier);
                                }
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemasInfo;
    }

    static Schema buildIcebergSchema(
            MySqlTableInfo mySqlTableInfo,
            boolean caseSensitive) {
        MySqlSchema mySqlSchema = mySqlTableInfo.schema();
        LinkedHashMap<String, JdbcField> mySqlFields;
        List<String> mySqlPrimaryKeys;
        
        if (caseSensitive) {
            mySqlFields = mySqlSchema.fields();
            mySqlPrimaryKeys = mySqlSchema.primaryKeys();
        } else {
            mySqlFields = new LinkedHashMap<>();
            for (Map.Entry<String, JdbcField> entry :
                    mySqlSchema.fields().entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !mySqlFields.containsKey(fieldName.toLowerCase()),
                        String.format(
                                "Duplicate key '%s' in table '%s' appears when converting fields map keys to case-insensitive form.",
                                fieldName, mySqlTableInfo.location()));
                mySqlFields.put(fieldName.toLowerCase(), entry.getValue());
            }
            mySqlPrimaryKeys =
                    mySqlSchema.primaryKeys().stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());
        }

        // Columns
        List<NestedField> fields = new ArrayList<>();
        for (Map.Entry<String, JdbcField> entry : mySqlFields.entrySet()) {
            // TODO: full type mapper
            NestedField field = null;
            JdbcField jdbcField = entry.getValue();
            switch (jdbcField.getType().toUpperCase()) {
                case "NUMBER":
                    field = NestedField.of(
                            jdbcField.getIndex(), true, jdbcField.getName(), 
                            IntegerType.get(), jdbcField.getComment());
                    break;
                case "VARCHAR":
                case "VARCHAR2":
                    field = NestedField.of(
                            jdbcField.getIndex(), true, jdbcField.getName(),
                            StringType.get(), jdbcField.getComment());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupport MySQL data type '%s[%s]'.",
                                    jdbcField.getName(),
                                    jdbcField.getType()
                            ));
            }
            fields.add(field);
        }

        // Primary keys
        Set<Integer> primaryKeyIds = new HashSet<>();
        if (mySqlPrimaryKeys.size() > 0) {
            for (String key: mySqlPrimaryKeys) {
                primaryKeyIds.add(mySqlFields.get(key).getIndex());
            }
        } else {
            throw new IllegalArgumentException(
                    "Primary keys are not specified. "
                            + "Also, can't infer primary keys from MySQL table schemas because "
                            + "MySQL tables have no primary keys or have different primary keys.");
        }
        return new Schema(fields, primaryKeyIds);
    }

    public static MySqlSource<String> buildeMySqlSource(Configuration mySqlConfig, String tableList) {
        validateMySQLConfig(mySqlConfig);
        MySqlSourceBuilder<String> builder = MySqlSource.builder();
        builder.hostname(mySqlConfig.get(MySqlSourceOptions.HOSTNAME))
                .port(mySqlConfig.get(MySqlSourceOptions.PORT))
                .username(mySqlConfig.get(MySqlSourceOptions.USERNAME))
                .password(mySqlConfig.get(MySqlSourceOptions.PASSWORD))
                .databaseList(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME))
                .tableList(tableList);

        mySqlConfig.getOptional(MySqlSourceOptions.SERVER_ID)
                .ifPresent(builder::serverId);
        mySqlConfig.getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(builder::serverTimeZone);
        // MySQL CDC using increment snapshot, splitSize is used instead of fetchSize (as in JDBC
        // connector). splitSize is the number of records in each snapshot split.
        mySqlConfig.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(builder::splitSize);
        mySqlConfig.getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(builder::connectTimeout);
        mySqlConfig.getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(builder::connectMaxRetries);
        mySqlConfig.getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(builder::connectionPoolSize);
        mySqlConfig.getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(builder::heartbeatInterval);

        // see
        // https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/table/MySqlTableSourceFactory.java#L196
        String startupMode = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_MODE);
        if ("initial".equalsIgnoreCase(startupMode)) {
            builder.startupOptions(StartupOptions.initial());
        } else if ("earliest".equalsIgnoreCase(startupMode)) {
            builder.startupOptions(StartupOptions.earliest());
        } else if ("latest".equalsIgnoreCase(startupMode)) {
            builder.startupOptions(StartupOptions.latest());
        } else if ("specific-offset".equalsIgnoreCase(startupMode)) {
            BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();
            String file = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
            Long pos = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
            if (file != null && pos != null) {
                offsetBuilder.setBinlogFilePosition(file, pos);
            }
            mySqlConfig.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                    .ifPresent(offsetBuilder::setGtidSet);
            mySqlConfig.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                    .ifPresent(offsetBuilder::setSkipEvents);
            mySqlConfig.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                    .ifPresent(offsetBuilder::setSkipRows);
            builder.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            builder.startupOptions(
                    StartupOptions.timestamp(
                            mySqlConfig.get(
                                    MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
        }

        Properties jdbcProperties = new Properties();
        Properties debeziumProperties = new Properties();
        for (Entry<String, String> entry : mySqlConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(JdbcUrlUtils.PROPERTIES_PREFIX)) {
                jdbcProperties.put(key.substring(JdbcUrlUtils.PROPERTIES_PREFIX.length()), value);
            } else if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        builder.jdbcProperties(jdbcProperties);
        builder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        boolean scanNewlyAddedTables = mySqlConfig.get(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);

        return builder
                .deserializer(schema)
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTables)
                .build();
    }

    private static void validateMySQLConfig(Configuration mySqlConfig) {
        Preconditions.checkArgument(
                mySqlConfig.get(MySqlSourceOptions.HOSTNAME) != null,
                String.format("mysql-conf [%s] must be specified.", MySqlSourceOptions.HOSTNAME.key())
        );
        Preconditions.checkArgument(
                mySqlConfig.get(MySqlSourceOptions.USERNAME) != null,
                String.format("mysql-conf [%s] must be specified.", MySqlSourceOptions.USERNAME.key())
        );
        Preconditions.checkArgument(
                mySqlConfig.get(MySqlSourceOptions.PASSWORD) != null,
                String.format("mysql-conf [%s] must be specified.", MySqlSourceOptions.PASSWORD.key())
        );
        Preconditions.checkArgument(
                mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME) != null,
                String.format("mysql-conf [%s] must be specified.", MySqlSourceOptions.DATABASE_NAME.key())
        );
    }

}
