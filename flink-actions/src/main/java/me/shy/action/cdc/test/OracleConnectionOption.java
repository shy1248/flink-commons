package me.shy.action.cdc.test;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import java.util.List;

public class OracleConnectionOption {

    private String hostName;
    private int port;
    private String databaseName;
    private String userName;
    private String password;

    /** 是否支持logmine */
    private boolean useLogmine;

    private String outServerName;

    private List<String> tableNames;

    private String schemaName;

    private StartupOptions startupOption;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isUseLogmine() {
        return useLogmine;
    }

    public void setUseLogmine(boolean useLogmine) {
        this.useLogmine = useLogmine;
    }

    public String getOutServerName() {
        return outServerName;
    }

    public void setOutServerName(String outServerName) {
        this.outServerName = outServerName;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public StartupOptions getStartupOption() {
        return startupOption;
    }

    public void setStartupOption(StartupOptions startupOption) {
        this.startupOption = startupOption;
    }
}
