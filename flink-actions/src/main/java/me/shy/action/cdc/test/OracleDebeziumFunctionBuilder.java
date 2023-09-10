package me.shy.action.cdc.test;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import java.util.Properties;

public class OracleDebeziumFunctionBuilder {

    public DebeziumSourceFunction build(OracleConnectionOption option) {

        OracleSource.Builder builder = OracleSource.builder();
        builder.hostname(option.getHostName());

        builder.port(option.getPort());

        builder.username(option.getUserName());
        builder.password(option.getPassword());

        builder.database(option.getDatabaseName());

        String[] tableArray = new String[option.getTableNames().size()];
        int count = 0;
        for (String tableName : option.getTableNames()) {
            tableArray[count] = option.getSchemaName() + "." + tableName;
            count++;
        }

        String[] schemaArray = new String[]{option.getSchemaName()};
        builder.tableList(tableArray);
        builder.schemaList(schemaArray);

        // dbzProperties
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("database.tablename.case.insensitive", "false");
        if (option.isUseLogmine()) {
            dbzProperties.setProperty("log.mining.strategy", "online_catalog");
            dbzProperties.setProperty("log.mining.continuous.mine", "true");
        } else {
            dbzProperties.setProperty("database.connection.adpter", "xstream");
            dbzProperties.setProperty("database.out.server.name", option.getOutServerName());
        }
        builder.debeziumProperties(dbzProperties);
        builder.deserializer(new CustomDebeziumDeserializationSchema());
        builder.startupOptions(option.getStartupOption());
        return builder.build();
    }

}
