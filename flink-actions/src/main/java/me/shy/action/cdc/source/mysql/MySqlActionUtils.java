package me.shy.action.cdc.source.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import java.util.Map.Entry;
import java.util.Properties;
import jdk.nashorn.internal.ir.IfNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

public class MySqlActionUtils {
    
    public MySqlSource<String> buildeMySqlSource (Configuration mySqlConfig, String tableList) {
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
        for (Entry<String, String> entry: mySqlConfig.toMap().entrySet()) {
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

        return builder.build();
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
