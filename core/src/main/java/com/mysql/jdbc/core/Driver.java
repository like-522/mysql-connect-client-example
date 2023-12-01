package com.mysql.jdbc.core;
import com.mysql.jdbc.core.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * @author hjx
 */
public class Driver implements java.sql.Driver {

    /**
     * mysql协议前缀
     */
    private static final String URL_PREFIX = "jdbc:mysql://";

    /**
     * 要连接的数据库名
     */
    public static final String DBNAME_PROPERTY_KEY = "DBNAME";

    /**
     * 要连接数据的ip
     */
    public static final String HOST_PROPERTY_KEY = "HOST";

    /**
     * 要连接数据的端口
     */
    public static final String PORT_PROPERTY_KEY = "PORT";

    /**
     * 要连接数据库的用户名
     */
    public static final String USER_PROPERTY_KEY = "user";

    /**
     * 要连接数据库的密码
     */
    public static final String PASSWORD_PROPERTY_KEY = "password";

    static {
        try {
            java.sql.DriverManager.registerDriver(new Driver());
        } catch (SQLException E) {
            throw new RuntimeException("不能注册驱动!");
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Properties properties = null;
        if ((properties = parseURL(url, info)) == null) {
            return null;
        }
        try {
            Connection connection = new MysqlConnectionImpl(host(properties),port(properties),database(properties),url,properties);
            return connection;
        } catch (Exception ex){
            SQLException sqlEx = new SQLException(ex);
            throw sqlEx;
        }
    }


    @Override
    public boolean acceptsURL(String url) throws SQLException {
        //不实现
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        //不实现版本不同的差异问题
        return 0;
    }

    @Override
    public int getMinorVersion() {
        //不实现版本不同的差异问题
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * 解析url
     * @param url url
     * @param properties info信息
     * @return
     */
    protected Properties parseURL(String url, Properties properties) {
        if (url == null) {
            return null;
        }
        if (!StringUtils.startsWithIgnoreCase(url, URL_PREFIX)){
            return null;
        }

        int beginningOfHost = url.indexOf("//");

        // ? 后面是一些mysql参数配置.如编码配置.
        int paramIndex = url.indexOf("?");
        if (paramIndex != -1) {
            String paramString = url.substring(paramIndex + 1);
            url = url.substring(0, paramIndex);
            //每个参数配置用& 符号区分,例如useUnicode=true&characterEncoding=utf8
            StringTokenizer queryParams = new StringTokenizer(paramString, "&");
            while (queryParams.hasMoreTokens()) {
                String parameterValuePair = queryParams.nextToken();
                int indexOfEquals =  parameterValuePair.indexOf("=");
                if (indexOfEquals!=-1){
                    String key;
                    String value = null;

                    key = parameterValuePair.substring(0,indexOfEquals);
                    if (indexOfEquals + 1 < parameterValuePair.length()) {
                        value = parameterValuePair.substring(indexOfEquals+1);
                    }
                    if (value != null && value.length() > 0 && key.length() > 0) {
                        try {
                            properties.put(key, URLDecoder.decode(value,
                                    StandardCharsets.UTF_8.name()));
                        } catch (UnsupportedEncodingException | NoSuchMethodError badEncoding) {
                            properties.put(key, URLDecoder.decode(value));
                        }
                    }
                }
            }
        }
        url = url.substring(beginningOfHost + 2);
        String hostStuff;
        int daNameIndex = url.indexOf("/");
        if (daNameIndex!=-1){
            hostStuff = url.substring(0, daNameIndex);
            if ((daNameIndex + 1) < url.length()) {
                properties.put(DBNAME_PROPERTY_KEY,
                        url.substring(daNameIndex + 1));
            }
        }else {
            hostStuff = url;
        }
        int portIndex = hostStuff.indexOf(":");
        properties.put(HOST_PROPERTY_KEY,hostStuff.substring(0,portIndex));
        properties.put(PORT_PROPERTY_KEY,hostStuff.substring(portIndex+1));
        return properties;
    }

    /**
     * 获取端口
     * @param properties
     * @return
     */
    public Integer port(Properties properties) {
        return Integer.parseInt(properties.getProperty(PORT_PROPERTY_KEY, "3306"));
    }

    /**
     * 获取ip
     * @param properties
     * @return
     */
    public String host(Properties properties) {
        return properties.getProperty(HOST_PROPERTY_KEY, "localhost");
    }

    /**
     * 获取连接的数据库
     * @param properties
     * @return
     */
    private String database(Properties properties) {
       return properties.getProperty(DBNAME_PROPERTY_KEY);
    }
}
