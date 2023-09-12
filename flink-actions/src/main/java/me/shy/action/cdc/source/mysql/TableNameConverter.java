package me.shy.action.cdc.source.mysql;

import java.io.Serializable;
import org.apache.iceberg.catalog.TableIdentifier;

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

    public String convert(TableIdentifier originIdentifier) {
        String rawName =
                mergeShards
                        ? originIdentifier.name()
                        : originIdentifier.namespace()
                                + "_"
                                + originIdentifier.name();
        return convert(rawName);
    }
}

