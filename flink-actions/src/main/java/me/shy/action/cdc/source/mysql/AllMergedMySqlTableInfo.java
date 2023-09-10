package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import me.shy.action.cdc.source.Identifier;

public class AllMergedMySqlTableInfo implements MySqlTableInfo {

    private final List<Identifier> fromTables;
    private MySqlSchema schema;

    public AllMergedMySqlTableInfo() {
        this.fromTables = new ArrayList<>();
    }

    public void init(Identifier identifier, MySqlSchema schema) {
        this.fromTables.add(identifier);
        this.schema = schema;
    }

    public AllMergedMySqlTableInfo merge(Identifier otherTableId, MySqlSchema other) {
        schema = schema.merge(location(), otherTableId.getFullName(), other);
        fromTables.add(otherTableId);
        return this;
    }

    @Override
    public String location() {
        return String.format(
                "{%s}",
                fromTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));
    }

    @Override
    public List<Identifier> identifiers() {
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
