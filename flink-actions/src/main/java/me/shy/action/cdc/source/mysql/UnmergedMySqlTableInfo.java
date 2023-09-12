package me.shy.action.cdc.source.mysql;

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;

public class UnmergedMySqlTableInfo implements MySqlTableInfo {

    private final TableIdentifier identifier;
    private final MySqlSchema schema;

    public UnmergedMySqlTableInfo(TableIdentifier identifier, MySqlSchema schema) {
        this.identifier = identifier;
        this.schema = schema;
    }

    @Override
    public String location() {
        return identifier.toString();
    }

    @Override
    public List<TableIdentifier> identifiers() {
        return Collections.singletonList(identifier);
    }

    @Override
    public String tableName() {
        return identifier.name();
    }

    @Override
    public String toIcebergTableName() {
        // the Paimon table name should be compound of origin database name and table name
        // together to avoid name conflict
        return identifier.namespace() + "_" + identifier.name();
    }

    @Override
    public MySqlSchema schema() {
        return schema;
    }
}

