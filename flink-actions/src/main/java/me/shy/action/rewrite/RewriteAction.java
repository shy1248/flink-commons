package me.shy.action.rewrite;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteAction {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteAction.class);

    private final CatalogLoader catalogLoader;
    private final String database;
    
    public RewriteAction(            
            String catalogName,
            String catalogType,
            String database,
            Map<String, String> catalogConfig) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        // for testing 
        System.setProperty("java.security.krb5.conf",
                "C:\\Users\\shy\\Documents\\applications\\kerberos-4.1\\krb5.ini");
        System.setProperty("hadoop.home.dir",
                "C:\\Users\\shy\\Documents\\applications\\winutils-master\\Hadoop-2.10.0");
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("hadoop.rpc.protection", "Privacy");
        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab("shy/testbased@SHY.ME",
                    "C:\\Users\\shy\\Downloads\\hadoop.keytab");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (catalogType.equalsIgnoreCase("hadoop")) {
            this.catalogLoader = CatalogLoader.hadoop(catalogName, configuration, catalogConfig);
        } else if (catalogType.equalsIgnoreCase("hive")) {
            this.catalogLoader = CatalogLoader.hive(catalogName, configuration, catalogConfig);
        } else if (catalogType.equalsIgnoreCase("rest")) {
            this.catalogLoader = CatalogLoader.rest(catalogName, configuration, catalogConfig);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unsupport catalog type '%s'.", catalogType));
        }
        
        this.database = database;
    }

    public static void main(String[] args) {
        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("uri", "thrift://testbased:9083");
        catalogConfig.put("warehouse", "hdfs://testbased/data/hive");
        RewriteAction action = new RewriteAction("iceberg_clog", "hive", "demo", catalogConfig);
        TableIdentifier students = TableIdentifier.of(action.database, "students");
        Table table = action.catalogLoader.loadCatalog().loadTable(students);
        RewriteDataFilesActionResult result =
                Actions.forTable(table).rewriteDataFiles().targetSizeInBytes(512 * 1024 * 1024).execute();
        
        result.addedDataFiles().forEach(f -> {
            System.out.println("Added files: ");
            System.out.println("    => " + f.path());
        });

        result.deletedDataFiles().forEach(f -> {
            System.out.println("Deleted files: ");
            System.out.println("    => " + f.path());
        });
    }
}
