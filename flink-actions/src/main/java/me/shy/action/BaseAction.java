package me.shy.action;

import java.util.Map;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * @author shy
 * @date 2023/09/02 13:42
 **/
public abstract class BaseAction implements Action {

    protected void execute(StreamExecutionEnvironment env, String defaultName) throws Exception {
        ReadableConfig config = env.getConfiguration();
        String name = config.getOptional(PipelineOptions.NAME).orElse(defaultName);
        env.execute(name);
    }
}
