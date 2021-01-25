package org.forchange.online.translator;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.connectors.kudu.table.UpsertOperationMapper;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.forchange.online.constants.SqlType;
import org.forchange.online.sink.DynamicSink;
import org.forchange.online.sink.KuduSinkConfig;
import org.forchange.online.sink.SinkTableConfig;

import static org.forchange.online.constants.SqlType.validateSQLType;


/**
 * @fileName: KuduTableTranslator.java
 * @description: Kudu Table转换器
 * @author: by echo huang
 * @date: 2021/1/5 2:45 下午
 */

public class KuduTableTranslator implements FlinkSqlTranslator {


    private DynamicSink dynamicSink = new DynamicSink();

    @Override
    public void translator(SinkTableConfig sinkTableConfig, StreamTableEnvironment tableEnvironment, StreamExecutionEnvironment env) throws Exception {
        String kuduMasterAddress = "10.16.24.40:7051,10.16.24.41:7051,10.16.24.42:7051";
        StringBuilder jobName = new StringBuilder();
        String columns = sinkTableConfig.getColumns();
        AbstractCatalog catalog = new KuduCatalog("cdh_kudu", kuduMasterAddress);
        String defaultDatabase = catalog.getDefaultDatabase();
        String tableName = sinkTableConfig.getTableName();
        ObjectPath targetSinkPath = new ObjectPath(defaultDatabase, tableName);
        if (catalog.tableExists(targetSinkPath)) {
            String executeSql = sinkTableConfig.getExecuteSql();
            jobName.append(executeSql).append(" ");
            validateSQLType(executeSql, SqlType.SELECT);
            Table selectResult = tableEnvironment.sqlQuery(executeSql);
            DataStream<Tuple2<Boolean, Row>> dataStream = tableEnvironment.toRetractStream(selectResult, Row.class);
            // kudu写入配置，配置最终一致性
            KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(kuduMasterAddress)
                    .build();
            KuduSinkConfig<Tuple2<Boolean, Row>> kuduSinkConfig = new KuduSinkConfig<>();
            kuduSinkConfig.setKuduWriterConfig(writerConfig);
            kuduSinkConfig.setKuduOperationMapper(new UpsertOperationMapper(StringUtils.split(columns, ",")));
            kuduSinkConfig.setKuduTableInfo(KuduTableInfo.forTable(tableName));
            KuduSink<Tuple2<Boolean, Row>> kuduSink = dynamicSink.kuduSink(kuduSinkConfig);
            dataStream.addSink(kuduSink).name(tableName);

        }
        JobClient jobClient = env.executeAsync(jobName.toString());
    }
}
