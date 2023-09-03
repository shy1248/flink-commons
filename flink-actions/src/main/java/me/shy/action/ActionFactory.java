package me.shy.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ActionFactory {
    Logger LOG = LoggerFactory.getLogger(ActionFactory.class);
    
    void showHelp();
    
    String name();
    
    Optional<Action> create(MultipleParameterTool params);
    
    static Optional<Action> createAction(String[] args) {
        String actionName = args[0].toLowerCase().trim();
        ActionFactory actionFactory;
        try {
            actionFactory = FactoryUtil.discoverActionFactory(
                    ActionFactory.class.getClassLoader(), actionName);
        } catch (FactoryException e) {
            showDefaultHelp();
            throw new UnsupportedOperationException(
                    String.format("Unknown action '%s'. The root cause is:\n %s", actionName, e));
        }
        
        LOG.info("Starting run Flink Actions: [{}] ...", actionName);
        LOG.debug("===> Application arguments is: {}", String.join(" ", args));
        String[] actionArgs = Arrays.copyOfRange(args, 1, args.length);
        LOG.info("===> Action arguments is: {}", String.join(" ", actionArgs));
        
        MultipleParameterTool params = MultipleParameterTool.fromArgs(actionArgs);
        if (params.has("help")) {
            actionFactory.showHelp();
            return Optional.empty();
        }
        return actionFactory.create(params);
    }

    static void showDefaultHelp() {
        System.out.println("Usage: <action> [OPTIONS]");
        System.out.println();
        System.out.println("Available actions:");
        List<String> factoryNames =
                FactoryUtil.discoverActionNames(ActionFactory.class.getClassLoader());
        factoryNames.forEach(action -> System.out.println("  " + action));
        System.out.println("For detailed options of each action, run <action> --help");
    }

    default List<Map<String, String>> getPartitions(MultipleParameterTool params) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : params.getMultiParameter("partition")) {
            Map<String, String> kvs = parseCommaSeparatedKeyValues(partition);
            partitions.add(kvs);
        }

        return partitions;
    }

    default Map<String, String> optionalConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return Collections.emptyMap();
        }

        Map<String, String> config = new HashMap<>();
        for (String kvString : params.getMultiParameter(key)) {
            parseKeyValueString(config, kvString);
        }
        return config;
    }

    default void checkRequiredArgument(MultipleParameterTool params, String key) {
        Preconditions.checkArgument(
                params.has(key), "Argument '%s' is required. Run '<action> --help' for help.", key);
    }

    default String getRequiredValue(MultipleParameterTool params, String key) {
        checkRequiredArgument(params, key);
        return params.get(key);
    }

    static Map<String, String> parseCommaSeparatedKeyValues(String keyValues) {
        Map<String, String> kvs = new HashMap<>();
        for (String kvString : keyValues.split(",")) {
            parseKeyValueString(kvs, kvString);
        }
        return kvs;
    }

    static void parseKeyValueString(Map<String, String> map, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        map.put(kv[0].trim(), kv[1].trim());
    }
    
}
