package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import me.shy.action.BaseAction;
import me.shy.action.cdc.EventParser;
import me.shy.action.cdc.sink.iceberg.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlSyncDatabaseAction extends BaseAction {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSyncDatabaseAction.class);

    private final CatalogLoader catalogLoader;
    private final String database;
    private final Configuration mySqlConfig;

    private final Map<String, String> tableConfig = new HashMap<>();
    private boolean mergeShards = true;
    private String tablePrefix = "";
    private String tableSuffix = "";
    private String includingTables = ".*";
    @Nullable
    String excludingTables;
    private final List<TableIdentifier> excludedTables = new ArrayList<>();

    @Override
    protected void execute(StreamExecutionEnvironment env, String defaultName) throws Exception {
        super.execute(env, defaultName);
    }

    public MySqlSyncDatabaseAction(
            String catalogName,
            String catalogType,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        // for testing 
        System.setProperty("java.security.krb5.conf",
                "C:\\Users\\shy\\Documents\\applications\\kerberos-4.1\\krb5.ini");
        System.setProperty("hadoop.home.dir",
                "C:\\Users\\shy\\Documents\\applications\\winutils-master\\Hadoop-2.10.0");
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("hadoop.rpc.protection", "Privacy");
        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab("shy/testbased@SHY.ME",
                    "C:\\Users\\shy\\Downloads\\hadoop.keytab");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (catalogType.equalsIgnoreCase("hadoop")) {
            this.catalogLoader = CatalogLoader.hadoop(catalogName, configuration, catalogConfig);
        } else if (catalogType.equalsIgnoreCase("hive")) {
            this.catalogLoader = CatalogLoader.hive(catalogName, configuration, catalogConfig);
        } else if (catalogType.equalsIgnoreCase("rest")) {
            this.catalogLoader = CatalogLoader.rest(catalogName, configuration, catalogConfig);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unsupport catalog type '%s'.", catalogType));
        }

        this.database = database;
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
    }

    public MySqlSyncDatabaseAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig.putAll(tableConfig);
        return this;
    }

    public MySqlSyncDatabaseAction mergeShards(boolean mergeShards) {
        this.mergeShards = mergeShards;
        return this;
    }

    public MySqlSyncDatabaseAction withTablePrefix(@Nullable String tablePrefix) {
        if (tablePrefix != null) {
            this.tablePrefix = tablePrefix;
        }
        return this;
    }

    public MySqlSyncDatabaseAction withTableSuffix(@Nullable String tableSuffix) {
        if (tableSuffix != null) {
            this.tableSuffix = tableSuffix;
        }
        return this;
    }

    public MySqlSyncDatabaseAction includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingTables = includingTables;
        }
        return this;
    }

    public MySqlSyncDatabaseAction excludingTables(@Nullable String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        checkArgument(!mySqlConfig.contains(MySqlSourceOptions.TABLE_NAME),
                MySqlSourceOptions.TABLE_NAME.key()
                        + " cannot be set for mysql-sync-database.");

        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        MySqlSchemasInfo mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(mySqlConfig,
                        tableName -> shouldMonitorTable(tableName, includingPattern, excludingPattern),
                        excludedTables);

        logNonPkTables(mySqlSchemasInfo.nonPkTables());
        List<MySqlTableInfo> mySqlTableInfos = mySqlSchemasInfo.toMySqlTableInfos(mergeShards);

        checkArgument(mySqlTableInfos.size() > 0,
                "No tables found in MySQL database " + mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or MySQL database does not exist.");

        // Create Iceberg Tables
        TableNameConverter tableNameConverter =
                new TableNameConverter(false, mergeShards, tablePrefix, tableSuffix);
        Map<TableIdentifier, Schema> tables = new HashMap<>();
        for (MySqlTableInfo tableInfo : mySqlTableInfos) {
            Schema fromMySql = MySqlActionUtils.buildIcebergSchema(mySqlConfig, tableInfo, false);
            TableIdentifier table = TableIdentifier.of(
                    database,
                    tableNameConverter.convert(tableInfo.toIcebergTableName()));
            tables.put(table, fromMySql);
            try {
                // TODO: Partition support
                // Only format version 2 support upsert
                tableConfig.put(TableProperties.FORMAT_VERSION, "2");
                catalogLoader.loadCatalog().createTable(
                        table, fromMySql, PartitionSpec.unpartitioned(), tableConfig);
            } catch (AlreadyExistsException e) {
                LOG.warn("Table '{}' already exists.", table);
            }
        }

        MySqlSource<String> source =
                MySqlActionUtils.buildeMySqlSource(mySqlConfig, buildTableList());

        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);

        EventParser.Factory<String> parserFactory = () -> new MySqlDebeziumJsonEventParser(
                zoneId, false, tableNameConverter, includingPattern, excludingPattern);

        String database = this.database;
        FlinkCdcSyncDatabaseSinkBuilder<String> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<String>()
                        .withInput(env.fromSource(
                                source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withCatalogLoader(catalogLoader)
                        .withDatabase(database)
                        .withTables(tables);

        String sinkParallelism = tableConfig.get(FlinkWriteOptions.WRITE_PARALLELISM.key());
        sinkBuilder.withParallelism(sinkParallelism != null ? Integer.parseInt(sinkParallelism) : 1);
        sinkBuilder.build();
    }

    private void logNonPkTables(List<TableIdentifier> nonPkTables) {
        if (!nonPkTables.isEmpty()) {
            LOG.debug(
                    "Didn't find primary keys for tables '{}'. These tables won't be synchronized.",
                    nonPkTables.stream()
                            .map(TableIdentifier::toString)
                            .collect(Collectors.joining(",")));
            excludedTables.addAll(nonPkTables);
        }
    }

    private boolean shouldMonitorTable(
            String mySqlTableName, Pattern includingPattern, @Nullable Pattern excludingPattern) {
        boolean shouldMonitor = includingPattern.matcher(mySqlTableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(mySqlTableName).matches();
        }
        if (!shouldMonitor) {
            LOG.debug("Source table '{}' is excluded.", mySqlTableName);
        }
        return shouldMonitor;
    }

    private String buildTableList() {
        String separatorRex = "\\.";
        String includingPattern = String.format("(%s)%s(%s)",
                mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME), separatorRex, includingTables);
        if (excludedTables.isEmpty()) {
            return includingPattern;
        }
        String excludingPattern = excludedTables.stream()
                .map(t -> String.format("(^%s$)", t.namespace() + separatorRex + t.name()))
                .collect(Collectors.joining("|"));
        excludingPattern = "?!" + excludingPattern;
        return String.format("(%s)(%s)", excludingPattern, includingPattern);
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);
        build(env);
        execute(env, String.format("MySQL => Iceberg Database Sync: %s", database));
    }
}

