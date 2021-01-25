package org.forchange.online.sink;


import lombok.Data;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;

/**
 * @fileName: KuduSinkConfig.java
 * @description: Kudu Sink配置
 * @author: by echo huang
 * @date: 2020/11/10 3:11 下午
 */
@Data
public class KuduSinkConfig<T> {
    private KuduWriterConfig kuduWriterConfig;
    private KuduTableInfo kuduTableInfo;
    private KuduOperationMapper<T> kuduOperationMapper;
    private KuduFailureHandler kuduFailureHandler = new DefaultKuduFailureHandler();

}
