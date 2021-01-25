package org.forchange.online.driver.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.forchange.online.env.FlinkTableEnv;
import org.forchange.online.sink.SinkTableConfig;
import org.forchange.online.translator.FlinkSqlTranslator;
import org.forchange.online.translator.KuduTableTranslator;

/**
 * @author yangxu
 * @version 1.0
 * @date 2021/1/21 6:09 下午
 */
public class DemoDwdDriver {
    public static void main(String[] args) throws Exception {
        //初始化环境
        StreamExecutionEnvironment streamEnv = FlinkTableEnv.dataStreamRunEnv();
        StreamTableEnvironment tableEnv = FlinkTableEnv.getTableEnv(streamEnv);
        // 创建流表映射demo
        tableEnv.executeSql("create table kafka_source_for_os_questionnaire_lists(id STRING,\nuser_id STRING,\nquestionnaire_id STRING,\ntype STRING,\nquestion_id STRING,\noption_id STRING,\n`value` STRING,\ncreated_at string,\nupdated_at STRING,\nschedule_id STRING,primary key(id) NOT ENFORCED,proctime as PROCTIME())\nwith('connector'='kafka',\n'topic'='common_test.for_os.questionnaire_lists',\n'properties.bootstrap.servers'='10.16.24.43:9092,10.16.24.44:9092,10.16.24.45:9092',\n'properties.group.id'='kafka_source_for_os_groups',\n'scan.startup.mode'= 'earliest-offset',\n'format'='debezium-json')");
        //创建kudu维度表映射
        tableEnv.executeSql("create table kudu_sink_for_os_questionnaire_options(id STRING,\nquestionnaire_id STRING,\nparent_id STRING,\n`value` STRING,\nrequire STRING,\ndesc STRING,\noption_type STRING,\nextra STRING,\ncreated_at STRING,\nupdated_at STRING)with(\n'connector.type' = 'kudu','kudu.masters' = '10.16.24.40:7051,10.16.24.41:7051,10.16.24.42:7051','kudu.table' = 'kudu_sink_for_os_questionnaire_options1',\n'kudu.replicas'='3',\n'kudu.primary-key-columns'='id')");
        tableEnv.executeSql("create table kudu_sink_for_os_questionnaire(id STRING,\ntitle STRING,\nplaceholder STRING,\nkey STRING,\ntype STRING,\nrequire STRING,\nstatus STRING,\ncreated_at STRING,\nupdated_at STRING)with(\n'connector.type' = 'kudu','kudu.masters' = '10.16.24.40:7051,10.16.24.41:7051,10.16.24.42:7051','kudu.table' = 'kudu_sink_for_os_questionnaire',\n'kudu.hash-columns'='id',\n'kudu.replicas'='3',\n'kudu.primary-key-columns'='id')");
        //创建kudu结果表
        tableEnv.executeSql("drop table if EXISTS cdh_kudu.default_database.TestResultTable");
       tableEnv.executeSql("CREATE TABLE cdh_kudu.default_database.TestResultTable (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  type STRING ,\n" +
                "  title STRING \n" +
//                "  require STRING \n" +
                ") WITH (\n" +
                "  'kudu.hash-columns' = 'id',\n" +
                "  'kudu.primary-key-columns' = 'id'\n" +
                ")");
        //最终核心执行逻辑
        String executeSql = "SELECT ql.id,ql.user_id,ql.type,q.title  FROM kafka_source_for_os_questionnaire_lists as  ql LEFT JOIN kudu_sink_for_os_questionnaire FOR SYSTEM_TIME AS OF ql.proctime as  q ON ql.question_id = q.id ";
        //结果表字段集
        String columns = "id,user_id,type,title";
        //封装参数
//        tableEnv.executeSql("INSERT INTO TestResultTable SELECT ql.id,ql.user_id,ql.type,q.title,qo.require  FROM kafka_source_for_os_questionnaire_lists as  ql LEFT JOIN kudu_sink_for_os_questionnaire FOR SYSTEM_TIME AS OF ql.proctime as  q ON ql.question_id = q.id LEFT JOIN kudu_sink_for_os_questionnaire_options as qo on ql.option_id = qo.id");
        SinkTableConfig sinkTableConfig = new SinkTableConfig();
        sinkTableConfig.setExecuteSql(executeSql);
        sinkTableConfig.setColumns(columns);
        sinkTableConfig.setTableName("TestResultTable");

        //执行
        FlinkSqlTranslator flinkSqlTranslator = new KuduTableTranslator();
        flinkSqlTranslator.translator(sinkTableConfig,tableEnv,streamEnv);

        //写入打印流，可直接把结果打印在工作台
        //        tableEnv.executeSql("CREATE TABLE print_table WITH ('connector' = 'print')\n" +
//                "LIKE kudu_sink_for_os_questionnaire (EXCLUDING ALL)");
//        tableEnv.executeSql("insert into  print_table select id,user_id,questionnaire_id,type,question_id,option_id,`value`,created_at,updated_at,schedule_id from kafka_source_for_os_questionnaire_lists");
//        tableEnv.executeSql("insert into  print_table select * from kudu_sink_for_os_questionnaire");

    }
}
