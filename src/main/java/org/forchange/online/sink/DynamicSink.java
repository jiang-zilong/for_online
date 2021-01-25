package org.forchange.online.sink;

import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @fileName: DynamicSink.java
 * @description: 动态Sink组建
 * @author: by echo huang
 * @date: 2020/10/27 11:36 上午
 */

public class DynamicSink {

    /**
     * kafka producer memory store
     * <p>
     * key: topic+bootstrapServers
     */
    private static final Map<String, FlinkKafkaProducer<String>> PRODUCER_STORE = new ConcurrentHashMap<>();



    /**
     * kudu sink
     *
     * @param kuduSinkConfig kudu sink config
     * @param <T>            sink pojo type
     * @return {@link KuduSink<T>}
     */
    public <T> KuduSink<T> kuduSink(KuduSinkConfig<T> kuduSinkConfig) {
        return new KuduSink<T>(kuduSinkConfig.getKuduWriterConfig(), kuduSinkConfig.getKuduTableInfo(), kuduSinkConfig.getKuduOperationMapper(),
                kuduSinkConfig.getKuduFailureHandler());
    }

}
