package me.shy.action.sql;

import java.util.regex.Pattern;

public enum StatementType {
    SELECT("SELECT", "^SELECT.*"),

    CREATE("CREATE", "^CREATE(?!.*AS SELECT).*$"),

    DROP("DROP", "^DROP.*"),

    ALTER("ALTER", "^ALTER.*"),

    INSERT("INSERT", "^INSERT.*"),

    DESC("DESC", "^DESC.*"),

    DESCRIBE("DESCRIBE", "^DESCRIBE.*"),

    EXPLAIN("EXPLAIN", "^EXPLAIN.*"),

    USE("USE", "^USE.*"),

    SHOW("SHOW", "^SHOW.*"),

    LOAD("LOAD", "^LOAD.*"),

    UNLOAD("UNLOAD", "^UNLOAD.*"),

    SET("SET", "^SET.*"),

    UNSET("UNSET", "^UNSET.*"),

    RESET("RESET", "^RESET.*"),

    EXECUTE("EXECUTE", "^EXECUTE.*"),
    ADD_JAR("ADD_JAR", "^ADD\\s+JAR\\s+\\S+"),
    ADD("ADD", "^ADD\\s+CUSTOMJAR\\s+\\S+"),

    PRINT("PRINT", "^PRINT.*"),

    CTAS("CTAS", "^CREATE\\s.*AS\\sSELECT.*$"),

    UNKNOWN("UNKNOWN", "^UNKNOWN.*");

    private final String type;
    private final Pattern pattern;

    StatementType(String type, String regex) {
        this.type = type;
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }

    public String getType() {
        return type;
    }

    public boolean match(String statement) {
        return pattern.matcher(statement).matches();
    }
    
    static StatementType fromStatement(String statement) {
        for (StatementType type: StatementType.values()) {
            if (type.match(statement)) {
                return type;
            }
        }
        return StatementType.UNKNOWN;
    }
}
