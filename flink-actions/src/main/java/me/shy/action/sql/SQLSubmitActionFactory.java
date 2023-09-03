package me.shy.action.sql;

import me.shy.action.Action;
import me.shy.action.ActionFactory;
import java.util.Optional;
import org.apache.flink.api.java.utils.MultipleParameterTool;

/**
 * @author shy
 * @date 2023/09/02 14:17
 **/
public class SQLSubmitActionFactory implements ActionFactory {
    private final static String ACTION_NAME = "sql-submit";

    @Override
    public void showHelp() {
        System.out.println(
                "Action \"sql-submit\" submit sql statements from specified file to Flink." 
                        + "This is support run a pipeline in yarn-application mode, and variables replacement."
        );
        System.out.println();
        System.out.println("Syntax:");
        System.out.println();
        System.out.println("  sql-submit --sql-file <SQL-FILE> [--var <KEY=VALUE> [--var <KEY=VALUE> ...]]");
        System.out.println();
        System.out.println("--sql-file <SQL-FILE>  Required. SQL statements in this file will be executed.");
        System.out.println("--var <KEY=VALUE> Optional. In SQL statements which specified by '--sql-file <SQL-FILE>' can use '${KEY}' to define variable replacement.");
    }

    @Override
    public String name() {
        return ACTION_NAME;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "sql-file");
        SQLSubmitAction action = 
                new SQLSubmitAction(
                        getRequiredValue(params, "sql-file"))
                        .withVariables(optionalConfigMap(params, "var"));
        return Optional.of(action);
    }
}
