package org.forchange.online.env;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
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
    public static StreamTableEnvironment getTableEnv(StreamExecutionEnvironment environment){
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, streamSettings);
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, ofMinutes(3));
        tableEnvironment.getConfig().getConfiguration().set(MIN_PAUSE_BETWEEN_CHECKPOINTS, ofMinutes(1));
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTING_TIMEOUT, ofMinutes(5));
        KuduCatalog cdh_kudu = new KuduCatalog("cdh_kudu", "10.16.24.40:7051,10.16.24.41:7051,10.16.24.42:7051");
        tableEnvironment.registerCatalog("cdh_kudu",cdh_kudu);
        tableEnvironment.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        return tableEnvironment;

    }


    /**
     * 流执行环境
     *
     * @return {@link StreamExecutionEnvironment}
     */
    public static StreamExecutionEnvironment dataStreamRunEnv() {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认flink失败重启策略
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000 * 60 * 3));
        // 默认时间语义为EventTime,设置watermark生成间隔时间

            environment.getConfig().setAutoWatermarkInterval(200);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(3*60*1000);
        // 默认配置
        checkpointConfig.setMinPauseBetweenCheckpoints(5 * 1000 * 60);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(5*60*1000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(3 * 60 * 1000);

        return environment;
    }
}
