package me.shy.action.sql;

import me.shy.action.BaseAction;
import me.shy.action.constants.Constannts;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shy
 * @date 2023/09/02 14:17
 **/
public class SQLSubmitAction extends BaseAction {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSubmitAction.class);
    private final String jobStatementsFilePath;
    private Map<String, String> variablesMap;

    public SQLSubmitAction(String statementsFilePath) {
        this.jobStatementsFilePath = statementsFilePath;
    }

    public SQLSubmitAction withVariables(Map<String, String> variablesMap) {
        this.variablesMap = variablesMap;
        return this;
    }

    @Override
    public void run() throws Exception {
        List<Tuple2<StatementType, String>> jobStatements = loadStatements(jobStatementsFilePath);
        Configuration configuration = new Configuration();
        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        for (Tuple2<StatementType, String> statement : jobStatements) {
            try {

                String sql = statement.f1;
                StatementType type = statement.f0;
                switch (type) {
                    case SET:
                        LOG.info("Executing SET statement: {}", sql);
                        setOperation(tableEnvironment, sql);
                        break;
                    case SELECT:
                        LOG.info("Executing SELECT statement: {}", sql);
                        tableEnvironment.sqlQuery(sql).execute().print();
                        break;
                    case UNSET:
                    case EXPLAIN:
                    case UNKNOWN:
                        LOG.warn("Skipped unsupported SQL statement:\n {}", sql);
                        break;
                    default:
                        LOG.info("Executing {} statement: {}", type.getType(), sql);
                        tableEnvironment.executeSql(sql);
                }
            } catch (Exception e) {
                throw new Exception(String.format(
                        "Error found when trying to execute sql: %s", statement.f1), e);
            }
        }
    }

    private void setOperation(TableEnvironment env, String options) {
        String kvString = options.substring(StatementType.SET.getType().length()).trim();
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(String.format(
                    "Invalid key-value string '%s'. Please use format 'key=value'.", kvString));
        }
        String key = kv[0].trim();
        String value = kv[1].trim();
        TableConfig config = env.getConfig();
        Configuration newConfig = Configuration.fromMap(Collections.singletonMap(key, value));
        config.addConfiguration(newConfig);
        ConfigOption<String> option = ConfigOptions.key(key).stringType().noDefaultValue();
        LOG.info("Loading configuration property: {}, {}", key, config.get(option));
    }

    private List<Tuple2<StatementType, String>> loadStatements(String filePath) {
        String line;
        StringBuilder sqlBuffer = new StringBuilder();
        List<Tuple2<StatementType, String>> statementTuples = new ArrayList<>();
        try (BufferedReader reader = openFileReader(filePath)) {
            while ((line = reader.readLine()) != null) {
                // process comments
                if (line.contains(Constannts.DOUBLE_DASH)) {
                    int dashIndex = line.indexOf(Constannts.DOUBLE_DASH);
                    if (Math.max(0, dashIndex) == 0) {
                        line = "";
                    } else {
                        line = line.substring(0, dashIndex);
                    }
                }
                // empty line
                if (StringUtils.isNullOrWhitespaceOnly(line)) {
                    continue;
                }

                sqlBuffer.append(line).append("\n");
                if (line.endsWith(Constannts.SEMICOLONS)) {
                    String origStatement = sqlBuffer.toString();
                    String statement = sqlBuffer.substring(0, origStatement.indexOf(Constannts.SEMICOLONS));
                    statement = replaceVariable(statement);
                    parse(statement, statementTuples);
                    sqlBuffer.delete(0, sqlBuffer.length());
                }
            }
            return statementTuples;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void parse(String statement, List<Tuple2<StatementType, String>> statementTuples) {
        StatementType statementType = StatementType.fromStatement(statement);
        Tuple2<StatementType, String> statementTuple = new Tuple2<>();
        statementTuple.f0 = statementType;
        statementTuple.f1 = statement;
        statementTuples.add(statementTuple);
    }

    private String replaceVariable(String line) {
        Pattern p = Pattern.compile("\\$\\{(.+?)}");
        Matcher m = p.matcher(line);
        StringBuffer buffer = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            String value = variablesMap.getOrDefault(key, "");
            if (value == null || value.equals("")) {
                throw new IllegalArgumentException(String.format(
                        "Missing variable value for key '%s'. "
                                + "Please use option '--var %s=<VALUE>' to offer variable values.", key, key));
            }
            m.appendReplacement(buffer, "");
            buffer.append(value);
        }
        m.appendTail(buffer);
        return buffer.toString();
    }

    private BufferedReader openFileReader(String filePath) {
        try {
            URI uri = PackagedProgramUtils.resolveURI(filePath);
            String schema = uri.getScheme().toLowerCase();
            FileSystem fileSystem;
            if (schema.equals(Constannts.SCHEMA_HDFS)) {
                fileSystem = HadoopFileSystem.get(uri);
            } else if (schema.equals(Constannts.SCHEMA_LOCAL)) {
                fileSystem = HadoopFileSystem.getLocalFileSystem();
            } else {
                throw new IllegalArgumentException(
                        String.format("Unknown file system schema '%s'.", schema)
                );
            }

            Path path = new Path(uri);
            if (!fileSystem.exists(path)) {
                throw new IllegalArgumentException(
                        String.format("File %s dose not exists.", path.getPath())
                );
            }

            FSDataInputStream inputStream = fileSystem.open(path);
            return new BufferedReader(new InputStreamReader(inputStream));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid file path: %s.", filePath), e);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("An error found when trying to read file %s.", filePath), e);
        }
    }
}
