package org.forchange.online.env;

import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.*;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/**
 * @author yangxu
 * @version 1.0
 * @date 2021/1/21 5:57 下午
 */
public class FlinkTableEnv {
    public static StreamTableEnvironment getTableEnv(){
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, streamSettings);
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, ofMinutes(3));
        tableEnvironment.getConfig().getConfiguration().set(MIN_PAUSE_BETWEEN_CHECKPOINTS, ofMinutes(1));
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTING_TIMEOUT, ofMinutes(5));
        KuduCatalog cdh_kudu = new KuduCatalog("cdh_kudu", "cdh01:7051,cdh02:7051,cdh03:7051");
        tableEnvironment.registerCatalog("cdh_kudu",cdh_kudu);
        return tableEnvironment;

    }
}
