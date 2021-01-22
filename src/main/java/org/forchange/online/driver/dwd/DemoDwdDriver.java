package org.forchange.online.driver.dwd;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.forchange.online.env.FlinkTableEnv;

/**
 * @author yangxu
 * @version 1.0
 * @date 2021/1/21 6:09 下午
 */
public class DemoDwdDriver {
    public static void main(String[] args) {
        StreamTableEnvironment tableEnv = FlinkTableEnv.getTableEnv();
        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        tableEnvironment.useCatalog("cdh_kudu");
//        tableEnvironment.executeSql("create table table1(id STRING,\nquestionnaire_id STRING,\nparent_id STRING,\n`value` STRING,\nrequire STRING,\ndesc STRING,\noption_type STRING,\nextra STRING,\ncreated_at STRING,\nupdated_at STRING,update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,WATERMARK FOR update_time AS update_time)with(\n'connector.type' = 'kudu','kudu.masters' = 'cdh01:7051,cdh02:7051,cdh03:7051','kudu.table' = 'kudu_sink_for_os_questionnaire_options',\n'kudu.replicas'='3',\n'kudu.primary-key-columns'='id')");
        tableEnv.executeSql("create table kafka_source_for_os_questionnaire_lists(id STRING,\nuser_id STRING,\nquestionnaire_id STRING,\ntype STRING,\nquestion_id STRING,\noption_id STRING,\n`value` STRING,\ncreated_at string,\nupdated_at STRING,\nschedule_id STRING,primary key(id) NOT ENFORCED,proctime as PROCTIME())\nwith('connector'='kafka',\n'topic'='common_test.for_os.questionnaire_lists',\n'properties.bootstrap.servers'='cdh04:9092,cdh05:9092,cdh06:9092',\n'properties.group.id'='kafka_source_for_os_groups',\n'scan.startup.mode'= 'earliest-offset',\n'format'='debezium-json')");
        tableEnv.executeSql("create table kudu_sink_for_os_questionnaire_options(id STRING,\nquestionnaire_id STRING,\nparent_id STRING,\n`value` STRING,\nrequire STRING,\ndesc STRING,\noption_type STRING,\nextra STRING,\ncreated_at STRING,\nupdated_at STRING)with(\n'connector.type' = 'kudu','kudu.masters' = 'cdh01:7051,cdh02:7051,cdh03:7051','kudu.table' = 'kudu_sink_for_os_questionnaire_optionsgit1',\n'kudu.replicas'='3',\n'kudu.primary-key-columns'='id')");
        tableEnv.executeSql("create table kudu_sink_for_os_questionnaire(id STRING,\ntitle STRING,\nplaceholder STRING,\nkey STRING,\ntype STRING,\nrequire STRING,\nstatus STRING,\ncreated_at STRING,\nupdated_at STRING)with(\n'connector.type' = 'kudu','kudu.masters' = 'cdh01:7051,cdh02:7051,cdh03:7051','kudu.table' = 'kudu_sink_for_os_questionnaire',\n'kudu.hash-columns'='id',\n'kudu.replicas'='3',\n'kudu.primary-key-columns'='id')");
        tableEnv.sqlQuery("SELECT ql.id,ql.user_id,ql.type,q.title,qo.require  FROM kafka_source_for_os_questionnaire_lists as  ql LEFT JOIN kudu_sink_for_os_questionnaire FOR SYSTEM_TIME AS OF ql.proctime as  q ON ql.question_id = q.id LEFT JOIN kudu_sink_for_os_questionnaire_options as qo on ql.option_id = qo.id").execute().print();
//        tableEnv.executeSql("CREATE TABLE print_table WITH ('connector' = 'print')\n" +
//                "LIKE kafka_source_for_os_questionnaire_lists (EXCLUDING ALL)");
//        tableEnv.executeSql("insert into  print_table select id,user_id,questionnaire_id,type,question_id,option_id,`value`,created_at,updated_at,schedule_id from kafka_source_for_os_questionnaire_lists");
        tableEnv.from("print_table").execute().print();
    }
}
