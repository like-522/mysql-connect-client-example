package com.mysql.jdbc.core;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author hjx
 */
public interface MysqlConnection extends Connection {
    /**
     * 获取mysqlIO
     * @return
     */
    MysqlIO getIO();

    /**
     * 设置服务端缓存
     * @param serverPreparedStatement
     */
    void reCachePreparedStatement(ServerPreparedStatement serverPreparedStatement);

    /**
     * 执行  0x03: COM_QUERY 命令的sql
     * @param sql sql语句
     * @param database 数据库
     * @return
     */
    MysqlResultSet executeSQL(String sql, String database, Statement statement) throws SQLException;


    /**
     * 获取字符编码
     * @return
     */
    String getCharsetName();

    /**
     * 获取主键自增的偏移量
     * @return
     */
    int getAutoIncrementIncrement();
}
