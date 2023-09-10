package me.shy.action.cdc.source.mysql;

import java.io.Serializable;
import me.shy.action.cdc.source.Identifier;

public class TableNameConverter  implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean caseSensitive;
    private final boolean mergeShards;
    private final String prefix;
    private final String suffix;

    public TableNameConverter(boolean caseSensitive) {
        this(caseSensitive, true, "", "");
    }

    public TableNameConverter(
            boolean caseSensitive, boolean mergeShards, String prefix, String suffix) {
        this.caseSensitive = caseSensitive;
        this.mergeShards = mergeShards;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public String convert(String originName) {
        String tableName = caseSensitive ? originName : originName.toLowerCase();
        return prefix + tableName + suffix;
    }

    public String convert(Identifier originIdentifier) {
        String rawName =
                mergeShards
                        ? originIdentifier.getTableName()
                        : originIdentifier.getDatabaseName()
                                + "_"
                                + originIdentifier.getTableName();
        return convert(rawName);
    }
}

