package com.mysql.jdbc.core;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

/**
 * @author hjx
 */
public class PreparedStatementImpl extends StatementImpl implements PreparedStatement {

    //sql.
    protected String originalSql = null;

    //存储的sql相关信息
    ParseInfo parseInfo;

    //参数值
    byte[][] paramValues;

    //参数数量
    protected int parameterCount;

    public PreparedStatementImpl(MysqlConnection mysqlConnection, String sql, String database) {
        super(mysqlConnection,database);
        this.originalSql = sql;
        parseInfo = new ParseInfo(sql,mysqlConnection);
        parseInfo.lastUsed = System.currentTimeMillis();
        parseInfo.statementLength = sql.length();
        this.paramValues = new byte[parameterCount][];
    }

    public PreparedStatementImpl(MysqlConnection mysqlConnection, String sql, String database,ParseInfo parseInfo) {
        super(mysqlConnection,database);
        this.originalSql = sql;
        this.parseInfo = parseInfo;
    }

    public PreparedStatementImpl(MysqlConnection mysqlConnection,String sql, String database,int resultSetType, int resultSetConcurrency) {
        super(mysqlConnection,database);
        this.originalSql = sql;
        this.resultSetType =resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }

    public ParseInfo getParseInfo() {
        return parseInfo;
    }

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

    public void setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
    }

    class ParseInfo{
        //sql参数
        byte[][] staticSql;

        //sql长度
        int statementLength = 0;

        //最后使用时间
        long lastUsed = 0;

        public ParseInfo(){}

        public ParseInfo(String sql, MysqlConnection mysqlConnection) {
            try {
                List<byte[]> endpointList = new ArrayList<>();
                String charsetName = mysqlConnection.getCharsetName();
                if (charsetName == null){
                    charsetName = StandardCharsets.UTF_8.name();
                }
                if (sql.contains("?")){
                    int start = 0;
                    int end = 0;
                    for (char c:sql.toCharArray()){
                        end++;
                        if (c == '?'){
                            endpointList.add(sql.substring(start,end-1).getBytes(charsetName));
                            start = end;
                            parameterCount++;
                        }
                    }
                }else {
                    endpointList.add(sql.getBytes(charsetName));
                }
                staticSql = new byte[endpointList.size()][];
                for (int i = 0;i<endpointList.size();i++){
                    staticSql[i] = endpointList.get(i);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    protected void realClose(boolean closeOpenResults) throws SQLException {
        super.realClose(closeOpenResults);
        this.originalSql = null;
        this.paramValues = null;
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        MysqlConnection locallyScopedConn = this.connection;
        synchronized (locallyScopedConn) {
            this.firstSqlChar = this.originalSql.charAt(0);
            checkForDml(firstSqlChar);
            String  oldCatalog = null;
            if (!locallyScopedConn.getCatalog().equals(this.database)) {
                oldCatalog = locallyScopedConn.getCatalog();
                locallyScopedConn.setCatalog(this.database);
            }
            MysqlResultSet interceptorsResultSet = this.connection.getIO().invokeStatementInterceptorsPre(this.originalSql,this);
            if (interceptorsResultSet!=null){
                return interceptorsResultSet;
            }
            //执行预编译语句发送
            results = executeInternal(resultSetType,resultSetConcurrency);
            if (oldCatalog!=null){
                locallyScopedConn.setCatalog(oldCatalog);
            }
            interceptorsResultSet =  this.connection.getIO().invokeStatementInterceptorsPost(this.originalSql,this,results);
            if (interceptorsResultSet!=null){
                results = interceptorsResultSet;
            }
        }
        return results;
    }

    /**
     * 执行sql命令
     * @return
     * @throws SQLException
     */
    protected MysqlResultSet executeInternal(int resultSetType,int resultSetConcurrency) throws SQLException {
        MysqlIO mysql = this.connection.getIO();
        SqlBuffer packet = mysql.getSharedSendPacket();
        packet.writeByte((byte) MysqlDefs.COM_QUERY);
        if (parameterCount <= 0){
            byte[] field = parseInfo.staticSql[0];
            packet.writeBytesNoNull(field);
        }else {
            for (int i = 0; i <this.parseInfo.staticSql.length;i++){
                byte[] field = parseInfo.staticSql[i];
                byte[] value = paramValues[i];
                if (value == null){
                    throw new SQLException("第"+i+1+ "个参数值未设置");
                }
                packet.writeBytesNoNull(field);
                packet.writeBytesNoNull(value);
            }
        }
        SqlBuffer resultPacket =  mysql.sendCommand(MysqlDefs.COM_QUERY,null,0,packet,false);
        return mysql.readResultSet(resultPacket,false,resultSetType,resultSetConcurrency);
    }

    @Override
    public int executeUpdate() throws SQLException {
        //先做一下校验,后续待实现
        MysqlConnection locallyScopedConn = this.connection;
        synchronized (locallyScopedConn){
            if (locallyScopedConn.isReadOnly()){
                throw new SQLException("连接是只读的,只能进行查询.");
            }
            String  oldCatalog = null;
            if (!locallyScopedConn.getCatalog().equals(this.database)) {
                oldCatalog = locallyScopedConn.getCatalog();
                locallyScopedConn.setCatalog(this.database);
            }
            MysqlResultSet interceptorsResultSet = this.connection.getIO().invokeStatementInterceptorsPre(this.originalSql,this);
            if (interceptorsResultSet!=null){
                return interceptorsResultSet.getUpdateCount();
            }
            results = executeInternal(resultSetType,resultSetConcurrency);
            if (oldCatalog!=null){
                locallyScopedConn.setCatalog(oldCatalog);
            }
            interceptorsResultSet =  this.connection.getIO().invokeStatementInterceptorsPost(this.originalSql,this,results);
            if (interceptorsResultSet!=null){
                results = interceptorsResultSet;
            }
            if (results!=null){
                results.getUpdateCount();
            }
        }
        return 0;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
         setInternal(parameterIndex,"null");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setInternal(parameterIndex,String.valueOf(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setInternal(parameterIndex,String.valueOf(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setInternal(parameterIndex,String.valueOf(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
          if (parameterCount <=0){
              return;
          }
          setInternal(parameterIndex,String.valueOf(x));
    }

    /**
     * 设置参数的值
     * @param parameterIndex 参数位置
     * @param valueOf 值
     * @throws SQLException
     */
    private void setInternal(int parameterIndex, String valueOf) throws SQLException {
        if (parameterCount <=0){
            return;
        }
        int index = parameterIndex-1;
        checkBound(index);
        this.paramValues[index] = valueOf.getBytes();
    }

    protected void checkBound(int index) throws SQLException {
        if (index<0){
            throw new SQLException("parameterIndex不能小于1");
        }
        if (index>parameterCount){
            throw new SQLException("parameterIndex不能大于参数个数"+parameterCount);
        }
    }

    protected synchronized void setRetrieveGeneratedKeys(boolean retrieveGeneratedKeys) {
        this.retrieveGeneratedKeys = retrieveGeneratedKeys;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setInternal(parameterIndex,String.valueOf(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setInternal(parameterIndex,String.valueOf(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setInternal(parameterIndex,String.valueOf(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x==null){
            setNull(parameterIndex,0);
        }else {
            setInternal(parameterIndex,x.toString());
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setInternal(parameterIndex,x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (parameterCount <=0){
            return;
        }
        int index = parameterIndex-1;
        checkBound(index);
        this.paramValues[index] = x;
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, 0);
        }else {
            //简单实现.
            setInternal(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, 0);
        }else {
            //简单实现.
            setInternal(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void clearParameters() throws SQLException {
         this.parameterCount=0;
         this.paramValues=null;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        //不实现
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        //不实现
    }

    @Override
    public boolean execute() throws SQLException {
        MysqlConnection locallyScopedConn = this.connection;
        synchronized (locallyScopedConn) {
            String  oldCatalog = null;
            if (!locallyScopedConn.getCatalog().equals(this.database)) {
                oldCatalog = locallyScopedConn.getCatalog();
                locallyScopedConn.setCatalog(this.database);
            }
            //执行预编译语句发送
            results = executeInternal(resultSetType,resultSetConcurrency);
            if (oldCatalog!=null){
                locallyScopedConn.setCatalog(oldCatalog);
            }
        }
        if (results!=null){
            return true;
        }
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        //不实现
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        //不实现
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        //不实现
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        //不实现
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        //不实现
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        //不实现
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, 0);
        } else {
            SimpleDateFormat dateFormatter = new SimpleDateFormat(
                    "''yyyy-MM-dd''", Locale.CHINESE);
            if (cal!=null){
                cal.setTime(x);
                dateFormatter.setTimeZone(cal.getTimeZone());
            }
            setInternal(parameterIndex, dateFormatter.format(x));
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, 0);
        }else {
            //简单实现.
            setInternal(parameterIndex, "'" + x.toString() + "'");
        }
    }


    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, 0);
        }else {
            //简单实现.
            setInternal(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setInternal(parameterIndex,"null");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setInternal(parameterIndex,x.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        //不实现
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        //不实现
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        //不实现
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        //不实现
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        //不实现
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        //不实现
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        //不实现
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        //不实现
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        //不实现
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        //不实现
    }

    public void close() throws SQLException {
        realClose(true);
    }

}
