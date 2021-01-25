package org.forchange.online.sink;

import lombok.Data;

/**
 * @fileName: SinkTableConfig.java
 * @description: SinkTableConfig.java类说明
 * @author: by echo huang
 * @date: 2021/1/6 9:45 上午
 */
@Data
public class SinkTableConfig {
    private String ddl;
    private String tableName;
    private String columns;
    private String executeSql;
    private String sqlDialect;
    private Boolean enableDml;
    private Boolean recreated;


}
