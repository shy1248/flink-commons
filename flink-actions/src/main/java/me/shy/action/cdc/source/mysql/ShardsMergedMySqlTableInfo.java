package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;

public class ShardsMergedMySqlTableInfo implements MySqlTableInfo {

    private final List<String> fromDatabases;

    private String tableName;

    private MySqlSchema schema;

    public ShardsMergedMySqlTableInfo() {
        this.fromDatabases = new ArrayList<>();
    }

    public void init(TableIdentifier identifier, MySqlSchema schema) {
        this.fromDatabases.add(identifier.namespace().toString());
        this.tableName = identifier.name();
        this.schema = schema;
    }

    public ShardsMergedMySqlTableInfo merge(TableIdentifier otherTableId, MySqlSchema other) {
        checkArgument(
                otherTableId.name().equals(tableName),
                "Table to be merged '%s' should equals to current table name '%s'.",
                otherTableId.name(),
                tableName);

        schema = schema.merge(location(), otherTableId.toString(), other);
        fromDatabases.add(otherTableId.namespace().toString());
        return this;
    }

    @Override
    public String location() {
        return String.format("[%s].%s", String.join(",", fromDatabases), tableName);
    }

    @Override
    public List<TableIdentifier> identifiers() {
        return fromDatabases.stream()
                .map(databaseName -> TableIdentifier.of(databaseName, tableName))
                .collect(Collectors.toList());
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public String toIcebergTableName() {
        return tableName;
    }

    @Override
    public MySqlSchema schema() {
        return checkNotNull(schema, "MySqlSchema hasn't been set.");
    }
}

