package me.shy.action.cdc.source.mysql;

import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;

public interface MySqlTableInfo {

    /**
     * To indicate where is the table from.
     */
    String location();

    /**
     * Return all MySQL table identifiers that build this schema.
     */
    List<TableIdentifier> identifiers();

    String tableName();

    /**
     * Convert to corresponding Paimon table name.
     */
    String toIcebergTableName();

    MySqlSchema schema();
}
