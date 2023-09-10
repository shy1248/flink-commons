package me.shy.action.cdc.source.mysql;

import java.util.Collections;
import java.util.List;
import me.shy.action.cdc.source.Identifier;

public class UnmergedMySqlTableInfo implements MySqlTableInfo {

    private final Identifier identifier;
    private final MySqlSchema schema;

    public UnmergedMySqlTableInfo(Identifier identifier, MySqlSchema schema) {
        this.identifier = identifier;
        this.schema = schema;
    }

    @Override
    public String location() {
        return identifier.getFullName();
    }

    @Override
    public List<Identifier> identifiers() {
        return Collections.singletonList(identifier);
    }

    @Override
    public String tableName() {
        return identifier.getTableName();
    }

    @Override
    public String toIcebergTableName() {
        // the Paimon table name should be compound of origin database name and table name
        // together to avoid name conflict
        return identifier.getDatabaseName() + "_" + identifier.getTableName();
    }

    @Override
    public MySqlSchema schema() {
        return schema;
    }
}

