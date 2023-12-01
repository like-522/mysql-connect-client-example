package com.mysql.jdbc.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author hjx Statement执行前后执行一些操作
 */
public interface StatementInterceptor {

    /**
     * 初始化
     * @param conn 连接
     * @param props props
     * @throws SQLException
     */
    public void init(Connection conn, Properties props) throws SQLException;


    /**
     * 执行sql命令前执行该方法
     * @param sql
     * @param interceptedStatement
     * @param connection
     * @return
     * @throws SQLException
     */
    public  MysqlResultSet preProcess(String sql, Statement interceptedStatement, Connection connection)
            throws SQLException;

    /**
     * 执行sql命令后执行该方法
     * @param sql
     * @param interceptedStatement
     * @param originalResultSet
     * @param connection
     * @return
     * @throws SQLException
     */
    public  MysqlResultSet postProcess(String sql, Statement interceptedStatement, MysqlResultSet originalResultSet, Connection connection) throws SQLException;

    /**
     * 做一些销毁操作
     */
    public void destroy();
}
