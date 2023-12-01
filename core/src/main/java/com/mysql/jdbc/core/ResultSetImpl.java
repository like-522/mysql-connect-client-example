package com.mysql.jdbc.core;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author hjx
 */
public class ResultSetImpl  implements MysqlResultSet {
    //更新的行数
    private int updateCount;

    //更新的id
    private int  updateId;

    //服务信息
    private String serverInfo;

    //当前连接
    private Connection connection;

    //字段信息
    private Field[] fields;

    //行数据
    private RowData rowData;

    //是否关闭
    protected boolean isClosed = false;

    //当前行
    protected ResultSetRow thisRow = null;

    //是否检索有null值
    protected boolean wasNullFlag = false;

    //缓存列的索引
    protected Map<String,Integer> columnIndexCache;

    protected int resultSetType;
    protected int resultSetConcurrency;

    //
    protected boolean isBinaryEncoded;

    public ResultSetImpl(int updateCount, int updateId, String serverInfo, MysqlConnection connection,int resultSetType,int resultSetConcurrency,boolean isBinaryEncoded) {
        this.updateCount = updateCount;
        this.updateId = updateId;
        this.serverInfo  =  serverInfo;
        this.connection = connection;
        this.resultSetType =resultSetType;
        this.resultSetConcurrency   =resultSetConcurrency;
        this.isBinaryEncoded = isBinaryEncoded;
    }

    public ResultSetImpl(Field[] fields, RowData rowData, MysqlConnection connection,int resultSetType,int resultSetConcurrency,boolean isBinaryEncoded) {
        this.fields = fields;
        this.rowData = rowData;
        this.connection = connection;
        columnIndexCache = new HashMap<>(fields.length);
        this.resultSetType =resultSetType;
        this.resultSetConcurrency   =resultSetConcurrency;
        this.isBinaryEncoded = isBinaryEncoded;
        rowData.setMetadata(fields);
    }

    /**
     * 校验 columnIndex
     * @param columnIndex
     * @throws SQLException
     */
    private void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1){
            throw new SQLException("columnIndex不能小于1");
        }
        if (columnIndex>fields.length){
            throw new SQLException("columnIndex大于列最大长度" + fields.length);
        }
    }

    private String stringInternal(int columnIndex) throws SQLException {
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)){
            wasNullFlag =true;
            return null;
        }
        byte[] value =  this.thisRow.getColumnIndexByte(index);
        if (value.length<=0){
            return null;
        }
        return new String(value);
    }

    @Override
    public boolean next() throws SQLException {
        if (rowData == null){
            return false;
        }
        if (rowData.size() == 0){
            return false;
        }
        this.thisRow = this.rowData.next();
        if (this.thisRow != null){
           return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (isClosed){
            return;
        }
        this.serverInfo = null;
        this.thisRow = null;
        this.fields = null;
        this.rowData  = null;
        this.isClosed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNullFlag;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return stringInternal(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        this.checkColumnIndex(columnIndex);
        int index = columnIndex - 1;
        Field field = this.fields[index];
        byte[] value = this.thisRow.getColumnIndexByte(index);
        if (field.getMysqlType() == MysqlDefs.FIELD_TYPE_BIT) {
            if (value == null){
                wasNullFlag = true;
                return false;
            }
            byte boolVal = value[0];
            if (boolVal == (byte)'1') {
                return true;
            } else if (boolVal == (byte)'0') {
                return false;
            }
        }else {
            String str = new String(value);
            return str.equals("1") || str.equals("true");
        }
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        byte[] value = getBytes(columnIndex);
        return value[0];
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (this.isBinaryEncoded){
            return this.getNativeShort(columnIndex);
        }
        //简单实现
        String value = stringInternal(columnIndex);
        return value == null?0:Short.parseShort(value);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (this.isBinaryEncoded){
            return this.getNativeInt(columnIndex);
        }
        //简单实现
        String value = stringInternal(columnIndex);
        return value == null?0:Integer.parseInt(value);
    }



    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (this.isBinaryEncoded){
            return this.getNativeLong(columnIndex);
        }
        //简单实现
        String value = stringInternal(columnIndex);
        return value == null? 0 : Long.parseLong(value);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        //简单实现
        if (this.isBinaryEncoded){
            return this.getNativeFloat(columnIndex);
        }
        String value = stringInternal(columnIndex);
        return value == null? 0 : Float.parseFloat(value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        //简单实现
        if (this.isBinaryEncoded){
            return this.getNativeDouble(columnIndex);
        }
        String value = stringInternal(columnIndex);
        return value == null? 0 : Double.parseDouble(value);
    }

    protected int getNativeInt(int columnIndex) throws SQLException {
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)) {
            this.wasNullFlag = true;
            return 0;
        }
        this.wasNullFlag = false;
        Field f = this.fields[index];
        switch (f.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_TINY:
                return getNativeByte(columnIndex);

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                return getNativeShort(columnIndex);

            case MysqlDefs.FIELD_TYPE_INT24:
            case MysqlDefs.FIELD_TYPE_LONG:
                return this.thisRow.getNativeInt(index);

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return  (int)getNativeLong(columnIndex);

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return (int) getNativeDouble(columnIndex);

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return (int) getNativeFloat(columnIndex);

            default:
                return 0;
        }
    }

    private byte getNativeByte(int columnIndex) throws SQLException {
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)) {
            this.wasNullFlag = true;
            return 0;
        }
        this.wasNullFlag = false;
        byte[] bytes =  this.thisRow.getColumnIndexByte(index);
        return bytes[0];
    }

    private short getNativeShort(int columnIndex) throws SQLException {
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)) {
            this.wasNullFlag = true;
            return 0;
        }
        this.wasNullFlag = false;
        Field f = this.fields[index];
        switch (f.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_TINY:
                return getNativeByte(columnIndex);

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                return this.thisRow.getNativeShort(index);

            case MysqlDefs.FIELD_TYPE_INT24:
            case MysqlDefs.FIELD_TYPE_LONG:
                return (short) getNativeInt(columnIndex);

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return  (short)getNativeLong(columnIndex);

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return (short) getNativeDouble(columnIndex);

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return (short) getNativeFloat(columnIndex);

            default:
                return 0;
        }
    }

    private long getNativeLong(int columnIndex) throws SQLException{
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)) {
            this.wasNullFlag = true;
            return 0;
        }
        this.wasNullFlag = false;
        Field f = this.fields[index];
        switch (f.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_TINY:
                return getNativeByte(columnIndex);

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                return getNativeShort(columnIndex);

            case MysqlDefs.FIELD_TYPE_INT24:
            case MysqlDefs.FIELD_TYPE_LONG:
                return  getNativeInt(columnIndex);

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return  this.thisRow.getNativeLong(index);

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return (long) getNativeDouble(columnIndex);

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return (long) getNativeFloat(columnIndex);

            default:
                return 0;
        }
    }

    private double getNativeDouble(int columnIndex) throws SQLException{
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)) {
            this.wasNullFlag = true;
            return 0;
        }
        this.wasNullFlag = false;
        Field f = this.fields[index];
        switch (f.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_TINY:
                return getNativeByte(columnIndex);

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                return getNativeShort(columnIndex);

            case MysqlDefs.FIELD_TYPE_INT24:
            case MysqlDefs.FIELD_TYPE_LONG:
                return getNativeInt(columnIndex);

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return  getNativeLong(columnIndex);

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return this.thisRow.getNativeDouble(index);

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return getNativeFloat(columnIndex);

            default:
                return 0;
        }
    }

    private float getNativeFloat(int columnIndex) throws SQLException{
        this.checkColumnIndex(columnIndex);
        int index = columnIndex-1;
        if (this.thisRow.isNull(index)) {
            this.wasNullFlag = true;
            return 0;
        }
        this.wasNullFlag = false;
        Field f = this.fields[index];
        switch (f.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_TINY:
                return getNativeByte(columnIndex);

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                return getNativeShort(columnIndex);

            case MysqlDefs.FIELD_TYPE_INT24:
            case MysqlDefs.FIELD_TYPE_LONG:
                return (float) getNativeInt(columnIndex);

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return  (float)getNativeLong(columnIndex);

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return (float) getNativeDouble(columnIndex);

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return this.thisRow.getNativeFloat(index);

            default:
                return 0;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        this.checkColumnIndex(columnIndex);
        int index = columnIndex - 1;
        if (this.thisRow.isNull(index)){
            wasNullFlag = true;
            return new byte[0];
        }
        return this.thisRow.getColumnIndexByte(index);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex,null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return  getTime(columnIndex,null);

    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex,null);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return stringInternal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        //不实现
    }

    @Override
    public String getCursorName() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (columnIndexCache.containsKey(columnLabel)){
            return columnIndexCache.get(columnLabel);
        }
        for (int i=0;i<fields.length;i++){
            Field field = fields[i];
           if (field.getName().equalsIgnoreCase(columnLabel)){
                columnIndexCache.put(columnLabel,i+1);
                return i+1;
           }
        }
        throw new SQLException("columnLabel找不到");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.rowData.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.rowData.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.rowData.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
         return this.rowData.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.rowData.beforeFirst();
        this.thisRow = null;
    }

    @Override
    public void afterLast() throws SQLException {
        this.rowData.afterLast();
        this.thisRow = null;
    }

    @Override
    public boolean first() throws SQLException {
        boolean flag = true;
        if (this.rowData.isEmpty()) {
            flag = false;
        } else {
            this.rowData.beforeFirst();
            this.thisRow = this.rowData.next();
        }
        return flag;
    }

    @Override
    public boolean last() throws SQLException {
        boolean flag = true;
        if (this.rowData.isEmpty()) {
            flag = false;
        } else {
            this.rowData.beforeLast();
            this.thisRow = this.rowData.next();
        }
        return flag;
    }

    @Override
    public int getRow() throws SQLException {
        return  this.rowData.getCurrentRowNumber()+1;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        //不实现
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        //不实现
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        //不实现
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        //不实现
    }

    @Override
    public int getFetchDirection() throws SQLException {
        //不实现
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        //不实现
    }

    @Override
    public int getFetchSize() throws SQLException {
        //不实现
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        //不实现
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        //不实现
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        //不实现
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        //不实现
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        //不实现
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        //不实现
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        //不实现
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        //不实现
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        //不实现
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        //不实现
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        //不实现
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        //不实现
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        //不实现
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        //不实现
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        //不实现
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        //不实现
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        //不实现
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        //不实现
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        //不实现
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        //不实现
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        //不实现
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        //不实现
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        //不实现
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        //不实现
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        //不实现
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        //不实现
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        //不实现
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        //不实现
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        //不实现
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        //不实现
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        //不实现
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        //不实现
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        //不实现
    }

    @Override
    public void insertRow() throws SQLException {
        //不实现
    }

    @Override
    public void updateRow() throws SQLException {
        //不实现
    }

    @Override
    public void deleteRow() throws SQLException {
        //不实现
    }

    @Override
    public void refreshRow() throws SQLException {
        //不实现
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        //不实现
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        //不实现
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        //不实现
    }

    @Override
    public Statement getStatement() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkColumnIndex(columnIndex);
        int index = columnIndex - 1;
        if (this.isBinaryEncoded){
            return this.thisRow.getNativeDate(index, cal);
        }
        return this.thisRow.getDateFast(index, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel),cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkColumnIndex(columnIndex);
        int index = columnIndex - 1;
        if (this.isBinaryEncoded){
            return this.thisRow.getNativeTime(index, cal);
        }
        return this.thisRow.getTimeFast(index, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel),cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkColumnIndex(columnIndex);
        int index = columnIndex - 1;
        if (this.isBinaryEncoded) {
            return this.thisRow.getNativeTimestamp(index, cal);
        }
        return this.thisRow.getTimestampFast(index,cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel),cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        //不实现
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        //不实现
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        //不实现
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        //不实现
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        //不实现
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        //不实现
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        //不实现
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        //不实现
    }

    @Override
    public int getHoldability() throws SQLException {
        //不实现
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        //不实现
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        //不实现
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        //不实现
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        //不实现
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        //不实现
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        //不实现
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        //不实现
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        //不实现
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        //不实现
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        //不实现
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        //不实现
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        //不实现
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        //不实现
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        //不实现
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        //不实现
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        //不实现
        return null;
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
    public int getUpdateCount() {
        return this.updateCount;
    }

    @Override
    public int getInsertId() {
        return this.updateId;
    }
}
