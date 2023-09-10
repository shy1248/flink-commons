package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import me.shy.action.cdc.source.Identifier;

public class ShardsMergedMySqlTableInfo implements MySqlTableInfo {

    private final List<String> fromDatabases;

    private String tableName;

    private MySqlSchema schema;

    public ShardsMergedMySqlTableInfo() {
        this.fromDatabases = new ArrayList<>();
    }

    public void init(Identifier identifier, MySqlSchema schema) {
        this.fromDatabases.add(identifier.getDatabaseName());
        this.tableName = identifier.getTableName();
        this.schema = schema;
    }

    public ShardsMergedMySqlTableInfo merge(Identifier otherTableId, MySqlSchema other) {
        checkArgument(
                otherTableId.getTableName().equals(tableName),
                "Table to be merged '%s' should equals to current table name '%s'.",
                otherTableId.getTableName(),
                tableName);

        schema = schema.merge(location(), otherTableId.getFullName(), other);
        fromDatabases.add(otherTableId.getDatabaseName());
        return this;
    }

    @Override
    public String location() {
        return String.format("[%s].%s", String.join(",", fromDatabases), tableName);
    }

    @Override
    public List<Identifier> identifiers() {
        return fromDatabases.stream()
                .map(databaseName -> Identifier.create(databaseName, tableName))
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

