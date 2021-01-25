package org.forchange.online.translator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.forchange.online.sink.SinkTableConfig;


import java.util.List;

/**
 * @fileName: TableTranslator.java
 * @description: Flink Table对象转换器
 * @author: by echo huang
 * @date: 2021/1/5 2:42 下午
 */
public interface FlinkSqlTranslator {
    /**
     * table转换并且执行
     * @param sinkTableConfig   输出表配置
     * @param tableEnvironment  table执行环境
     * @param env              流执行环境
     */
    void translator(SinkTableConfig sinkTableConfig, StreamTableEnvironment tableEnvironment, StreamExecutionEnvironment env) throws Exception;
}
