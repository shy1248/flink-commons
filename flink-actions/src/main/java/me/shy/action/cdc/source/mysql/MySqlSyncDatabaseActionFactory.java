package me.shy.action.cdc.source.mysql;

import java.util.Optional;
import me.shy.action.Action;
import me.shy.action.ActionFactory;
import org.apache.flink.api.java.utils.MultipleParameterTool;

public class MySqlSyncDatabaseActionFactory implements ActionFactory {

    public static final String ACTION_NAME = "mysql-sync-iceberg";

    @Override
    public String name() {
        return ACTION_NAME;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "mysql-conf");

        MySqlSyncDatabaseAction mySqlSyncDatabaseAction =
                new MySqlSyncDatabaseAction(
                        getRequiredValue(params, "catalog-name"),
                        getRequiredValue(params, "catalog-type"),
                        getRequiredValue(params, "database"),
                        optionalConfigMap(params, "catalog-conf"),
                        optionalConfigMap(params, "mysql-conf"));

        mySqlSyncDatabaseAction
                .withTableConfig(optionalConfigMap(params, "table-conf"))
                .mergeShards(
                        !params.has("merge-shards")
                                || Boolean.parseBoolean(params.get("merge-shards")))
                .withTablePrefix(params.get("table-suffix"))
                .withTableSuffix(params.get("table-suffix"))
                .includingTables(params.get("including-tables"))
                .excludingTables(params.get("excluding-tables"));

        return Optional.of(mySqlSyncDatabaseAction);
    }

    @Override
    public void showHelp() {
        System.out.println(
                "Action \"mysql-sync-database\" creates a streaming job "
                        + "with a Flink MySQL CDC source and multiple Iceberg table sinks "
                        + "to synchronize a whole MySQL database into one Iceberg database.\n"
                        + "Only MySQL tables with primary keys will be considered. ");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql-sync-iceberg --catalog-name <catalog-name> --catalog-type <catalog-type> --database " 
                        + "<database-name> "
                        + "[--merge-shards <true/false>] "
                        + "[--table-prefix <Iceberg-table-prefix>] "
                        + "[--table-suffix <Iceberg-table-suffix>] "
                        + "[--including-tables <mysql-table-name|name-regular-expr>] "
                        + "[--excluding-tables <mysql-table-name|name-regular-expr>] "
                        + "[--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] "
                        + "[--catalog-conf <Iceberg-catalog-conf> [--catalog-conf <Iceberg-catalog-conf> ...]] "
                        + "[--table-conf <Iceberg-table-sink-conf> [--table-conf <Iceberg-table-sink-conf> ...]]");
        System.out.println();

        System.out.println(
                "--merge-shards is default true, in this case, if some tables in different databases have the same name, "
                        + "their schemas will be merged and their records will be synchronized into one Iceberg table. "
                        + "Otherwise, each table's records will be synchronized to a corresponding Iceberg table, "
                        + "and the Iceberg table will be named to 'databaseName_tableName' to avoid potential name conflict.");
        System.out.println();

        System.out.println(
                "--table-prefix is the prefix of all Iceberg tables to be synchronized. For example, if you want all "
                        + "synchronized tables to have \"ods_\" as prefix, you can specify `--table-prefix ods_`.");
        System.out.println("The usage of --table-suffix is same as `--table-prefix`");
        System.out.println();

        System.out.println(
                "--including-tables is used to specify which source tables are to be synchronized. "
                        + "You must use '|' to separate multiple tables. Regular expression is supported.");
        System.out.println(
                "--excluding-tables is used to specify which source tables are not to be synchronized. "
                        + "The usage is same as --including-tables.");
        System.out.println(
                "--excluding-tables has higher priority than --including-tables if you specified both.");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password' and 'database-name' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the MySQL database you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options");
        System.out.println();

        System.out.println("Iceberg catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println("All Iceberg sink table will be applied the same set of configurations.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://iceberg.apache.org/docs/latest/configuration/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql-sync-database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --mysql-conf hostname=127.0.0.1 \\\n"
                        + "    --mysql-conf username=root \\\n"
                        + "    --mysql-conf password=123456 \\\n"
                        + "    --mysql-conf database-name=source_db \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}

