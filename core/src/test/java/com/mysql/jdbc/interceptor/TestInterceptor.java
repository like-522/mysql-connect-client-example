package com.mysql.jdbc.interceptor;
import com.mysql.jdbc.core.MysqlResultSet;
import com.mysql.jdbc.core.StatementInterceptor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author hjx
 */
public class TestInterceptor implements StatementInterceptor {
    @Override
    public void init(Connection conn, Properties props) throws SQLException {
       // System.out.println("init");
    }

    @Override
    public MysqlResultSet preProcess(String sql, Statement interceptedStatement, Connection connection) throws SQLException {
        //System.out.println("preProcess");
        return null;
    }

    @Override
    public MysqlResultSet postProcess(String sql, Statement interceptedStatement, MysqlResultSet originalResultSet, Connection connection) throws SQLException {
        //System.out.println("postProcess");
        return null;
    }

    @Override
    public void destroy() {

    }
}
