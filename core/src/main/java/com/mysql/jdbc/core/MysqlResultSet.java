package com.mysql.jdbc.core;

import java.sql.ResultSet;

/**
 * @author hjx
 */
public interface MysqlResultSet extends ResultSet {

    /**
     * 获取更改的数量
     * @return
     */
    int getUpdateCount();

    /**
     * 获取更新的id
     * @return
     */
    int getInsertId();
}
