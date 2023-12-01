package com.mysql.jdbc.core;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author hjx
 */
public class MySqlDataSource implements DataSource {

    protected String host;

    protected int port = 3306;

    protected String username;

    protected String password;

    protected String database;

    protected String url;

    protected boolean explicitUrl;

    protected final static Driver mysqlDriver;
    static {
        try {
            mysqlDriver = new Driver();
        } catch (Exception E) {
            throw new RuntimeException(
                    "Can not load Driver class com.mysql.jdbc.Driver");
        }
    }

    public MySqlDataSource(){}

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(this.username, this.password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties properties =new Properties();
        if (username!=null){
            properties.setProperty(Driver.USER_PROPERTY_KEY,username);
        }
        if (password!=null){
            properties.setProperty(Driver.PASSWORD_PROPERTY_KEY,password);
        }
        return getConnection(properties);
    }

    protected  Connection getConnection(Properties prop) throws SQLException{
        if (!this.explicitUrl){
            StringBuilder jdbcUrl = new StringBuilder("jdbc:mysql://");
            if (this.host != null) {
                jdbcUrl.append(this.host);
            }
            jdbcUrl.append(":");
            jdbcUrl.append(this.port);
            jdbcUrl.append("/");
            if (this.database != null) {
                jdbcUrl.append(this.database);
            }
            return mysqlDriver.connect(jdbcUrl.toString(),prop);
        }else {
           return mysqlDriver.connect(this.url,prop);
        }
    }

    public void setUrl(String url) {
        this.url = url;
        this.explicitUrl  = true;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabase(String database) {
        this.database = database;
    }


    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        //不实现
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        //不实现
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        //不实现
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        //不实现
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        //不实现
        return null;
    }


}
