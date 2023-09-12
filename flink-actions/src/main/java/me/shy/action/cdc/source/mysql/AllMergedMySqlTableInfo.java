package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;

public class AllMergedMySqlTableInfo implements MySqlTableInfo {

    private final List<TableIdentifier> fromTables;
    private MySqlSchema schema;

    public AllMergedMySqlTableInfo() {
        this.fromTables = new ArrayList<>();
    }

    public void init(TableIdentifier identifier, MySqlSchema schema) {
        this.fromTables.add(identifier);
        this.schema = schema;
    }

    public AllMergedMySqlTableInfo merge(TableIdentifier otherTableId, MySqlSchema other) {
        schema = schema.merge(location(), otherTableId.toString(), other);
        fromTables.add(otherTableId);
        return this;
    }

    @Override
    public String location() {
        return String.format(
                "{%s}",
                fromTables.stream().map(TableIdentifier::toString).collect(Collectors.joining(",")));
    }

    @Override
    public List<TableIdentifier> identifiers() {
        throw new UnsupportedOperationException(
                "AllMergedRichMySqlSchema doesn't support converting to identifiers.");
    }

    @Override
    public String tableName() {
        throw new UnsupportedOperationException(
                "AllMergedRichMySqlSchema doesn't support getting table name.");
    }

    @Override
    public String toIcebergTableName() {
        throw new UnsupportedOperationException(
                "AllMergedRichMySqlSchema doesn't support converting to Paimon table name.");
    }

    @Override
    public MySqlSchema schema() {
        return checkNotNull(schema, "MySqlSchema hasn't been set.");
    }
}
