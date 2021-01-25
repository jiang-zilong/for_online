package org.forchange.online.constants;



/**
 * @fileName: SqlType.java
 * @description: SqlType.java类说明
 * @author: by echo huang
 * @date: 2020/12/23 6:09 下午
 */
public enum SqlType {
    SELECT,
    INSERT;

    /**
     * 校验sql类型
     *
     * @param sql
     * @param sqlType
     */
    public static void validateSQLType(String sql, SqlType sqlType) throws Exception {
        if (!sql.toUpperCase().trim().startsWith(sqlType.name())) {
            throw new Exception("sql:" + sql + " sqlType:" + sqlType.name() + "，SQL与支持类型不一致");
        }
    }
}
