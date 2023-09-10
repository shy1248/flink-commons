package me.shy.action.cdc.source;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.util.StringUtils;

public class Identifier implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String UNKNOWN_DATABASE = "unknown";
    private final String database;
    private final String table;

    public Identifier(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public String getDatabaseName() {
        return database;
    }

    public String getTableName() {
        return table;
    }

    public String getFullName() {
        return UNKNOWN_DATABASE.equals(this.database)
                ? table
                : String.format("%s.%s", database, table);
    }

    public String getEscapedFullName() {
        return getEscapedFullName('`');
    }

    public String getEscapedFullName(char escapeChar) {
        return String.format(
                "%c%s%c.%c%s%c", escapeChar, database, escapeChar, escapeChar, table, escapeChar);
    }

    public static Identifier create(String db, String table) {
        return new Identifier(db, table);
    }

    public static Identifier fromString(String fullName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

        String[] paths = fullName.split("\\.");

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get splits from '%s' to get database and table", fullName));
        }

        return new Identifier(paths[0], paths[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Identifier that = (Identifier) o;
        return Objects.equals(database, that.database) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return "Identifier{" + "database='" + database + '\'' + ", table='" + table + '\'' + '}';
    }
}

