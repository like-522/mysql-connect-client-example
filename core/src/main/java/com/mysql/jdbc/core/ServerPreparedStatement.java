package com.mysql.jdbc.core;
import com.mysql.jdbc.core.util.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.TimeZone;

/**
 * @author hjx
 */
public class ServerPreparedStatement extends PreparedStatementImpl {
    /**
     * 是否缓存
     */
    protected boolean isCached = false;

    /** 声明id */
    private long serverStatementId;

    //字段数量
    private int fieldCount;

    //参数字段
    private Field[] parameterFields;

    //返回字段
    private Field[] resultFields;

    //参数绑定的值
    private BindValue[] parameterBindings;

    //是否有长参数标识, clob,blob
    private boolean detectedLongParameterSwitch = false;

    //是否需要发送参数类型给mysql服务
    private boolean sendTypesToServer = false;

    /**
     * 创建 一个预声明示例
     * @param mysqlConnection sql connection
     * @param sql sql
     * @param database 数据库
     * @param resultSetType 返回设置类型
     * @param resultSetConcurrency
     * @return
     */
    public  ServerPreparedStatement(MysqlConnection mysqlConnection, String sql, String database, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(mysqlConnection,sql,database,resultSetType,resultSetConcurrency);
        serverPrepare(sql);
    }

    /**
     * 发送需要预编译的语句
     * @param sql 预编译的sql
     * @throws SQLException
     */
    private synchronized void serverPrepare(String sql) throws SQLException {
        synchronized (this.connection) {
            MysqlIO mysql = this.connection.getIO();
            try {
                SqlBuffer prepareResultPacket = mysql.sendCommand(
                        MysqlDefs.COM_PREPARE, sql,0,null,false);

                prepareResultPacket.readByte();//跳过状态字节
                this.serverStatementId = prepareResultPacket.readLong();
                this.fieldCount = prepareResultPacket.readInt();
                this.parameterCount = prepareResultPacket.readInt();
                this.parameterBindings = new BindValue[this.parameterCount];
                if (this.parameterCount > 0) {
                    this.parameterFields = new Field[this.parameterCount];
                    SqlBuffer metaDataPacket = mysql.readPacket();
                    int i = 0;
                    while (!metaDataPacket.isLastDataPacket()
                            && (i < this.parameterCount)) {
                        this.parameterFields[i++] = mysql.unpackField(
                                metaDataPacket);
                        metaDataPacket = mysql.readPacket();
                    }
                }
                if (this.fieldCount > 0) {
                    this.resultFields = new Field[this.fieldCount];
                    SqlBuffer fieldPacket = mysql.readPacket();
                    int i = 0;
                    while (!fieldPacket.isLastDataPacket()
                            && (i < this.fieldCount)) {
                        this.resultFields[i++] = mysql.unpackField(fieldPacket);
                        fieldPacket = mysql.readPacket();
                    }
                }
            }catch (SQLException e){
                e.printStackTrace();
            }finally {
                this.connection.getIO().clearInputStream();
            }
        }
    }

    /**
     * 执行服务端预编译的语句
     * @return
     * @throws SQLException
     */
    protected MysqlResultSet executeInternal(int resultSetType,int resultSetConcurrency) throws SQLException {
        try {
            MysqlIO mysql = this.connection.getIO();
            SqlBuffer packet = mysql.getSharedSendPacket();
            packet.writeByte((byte) MysqlDefs.COM_STMT_EXECUTE);
            packet.writeLong(this.serverStatementId);
            packet.writeByte((byte) 0);
            packet.writeLong(1);

            //计算出当前参数的数量需要占几个字节.
            int nullBitLength = (this.parameterCount + 7)/8;
            int nullBitsPosition = packet.getPosition();
            //参数长度占位字节,如果参数有null值, 参数位置的位图 位为1
            for (int i=0;i<nullBitLength;i++){
                packet.writeByte((byte) 0);
            }
            byte[] nullBitsBuffer = new byte[nullBitLength];

            for (int i= 0;i<parameterBindings.length;i++){
                BindValue bindValue = parameterBindings[i];
                if (!bindValue.isSet){
                    throw new SQLException("第"+i+1+"参数值未设置..");
                }
            }
            //是否重新绑定参数 的类型
            packet.writeByte(this.sendTypesToServer ? (byte) 1 : (byte) 0);
            //发送参数类型
            if (sendTypesToServer){
                for (int i = 0;i<parameterCount;i++){
                    packet.writeInt(this.parameterBindings[i].bufferType);
                }
            }
            //发送参数值
            for (int i = 0; i < this.parameterCount; i++) {
                if (!this.parameterBindings[i].isLongData) {
                    if (!this.parameterBindings[i].isNull) {
                        storeBinding(packet, this.parameterBindings[i]);
                    } else {
                        //使用字节的位图来标识参数的null值,
                        nullBitsBuffer[i / 8] |= (1 << (i & 7));
                    }
                }
            }

            //设置  参数长度占位字节的字节value
            int endPosition = packet.getPosition();
            packet.setPosition(nullBitsPosition);
            packet.writeBytesNoNull(nullBitsBuffer);
            packet.setPosition(endPosition);
            SqlBuffer resultBuffer = mysql.sendCommand(MysqlDefs.COM_STMT_EXECUTE,null,0,packet,false);
            this.results = mysql.readResultSet(resultBuffer,true,resultSetType,resultSetConcurrency);
            this.sendTypesToServer = false;
            return results;
        }catch (SQLException e){
            e.printStackTrace();
            throw new SQLException("ServerPreparedStatement executeInternal err.");
        }finally {
            if (!this.isClosed && this.isCached){
                this.connection.reCachePreparedStatement(this);
            }
        }
    }

    /**
     * 绑定参数的值 到发送命令包中
     * @param packet 命令包
     * @param bindValue 参数绑定的值
     */
    private void storeBinding(SqlBuffer packet, BindValue bindValue) throws SQLException {
        try {
            Object value = bindValue.value;
            switch (bindValue.bufferType) {
                case MysqlDefs.FIELD_TYPE_TINY:
                    packet.writeByte((byte) bindValue.longBinding);
                return;

                case MysqlDefs.FIELD_TYPE_SHORT:
                    packet.writeInt((short)bindValue.longBinding);
                    return;

                case MysqlDefs.FIELD_TYPE_LONG:
                    packet.writeLong((int)bindValue.longBinding);
                    return;

                case MysqlDefs.FIELD_TYPE_LONGLONG:
                    packet.writeLongLong(bindValue.longBinding);
                    return;

                case MysqlDefs.FIELD_TYPE_FLOAT:
                    packet.writeFloat(bindValue.floatBinding);
                    return;

                case MysqlDefs.FIELD_TYPE_DOUBLE:
                    packet.ensureCapacity(8);
                    packet.writeDouble(bindValue.doubleBinding);
                    return;

                case MysqlDefs.FIELD_TYPE_TIME:
                    storeTime(packet, (Time) value);
                    return;

                case MysqlDefs.FIELD_TYPE_DATE:
                case MysqlDefs.FIELD_TYPE_DATETIME:
                case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                    storeDateTime(packet, (java.util.Date) value, bindValue.bufferType);
                    return;

                case MysqlDefs.FIELD_TYPE_VAR_STRING:
                case MysqlDefs.FIELD_TYPE_STRING:
                case MysqlDefs.FIELD_TYPE_VARCHAR:
                case MysqlDefs.FIELD_TYPE_DECIMAL:
                case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                    if (value instanceof byte[]) {
                        packet.writeBytesLength((byte[]) value);
                    }else {
                        packet.writeBytesLength(Objects.requireNonNull(StringUtils.getBytes((String) value)));
                    }
            }
        }catch (SQLException sq){
            throw new SQLException("绑定参数失败");
        }
    }

    //存储日期加时间格式 年月日 时分秒
    private synchronized void storeDateTime(SqlBuffer intoBuf, java.util.Date dt, int bufferType)
            throws SQLException {
        Calendar sessionCalendar =	new GregorianCalendar(TimeZone.getDefault());
        synchronized (sessionCalendar) {
            java.util.Date oldTime = sessionCalendar.getTime();
            try {
                sessionCalendar.setTime(dt);
                if (dt instanceof java.sql.Date) {
                    sessionCalendar.set(Calendar.HOUR_OF_DAY, 0);
                    sessionCalendar.set(Calendar.MINUTE, 0);
                    sessionCalendar.set(Calendar.SECOND, 0);
                }

                byte length = (byte) 7;
                if (dt instanceof java.sql.Timestamp) {
                    length = (byte) 11;
                }
                intoBuf.ensureCapacity(length);
                intoBuf.writeByte(length); // length
                int year = sessionCalendar.get(Calendar.YEAR);
                int month = sessionCalendar.get(Calendar.MONTH) + 1;
                int date = sessionCalendar.get(Calendar.DAY_OF_MONTH);

                intoBuf.writeInt(year);
                intoBuf.writeByte((byte) month);
                intoBuf.writeByte((byte) date);

                if (dt instanceof java.sql.Date) {
                    intoBuf.writeByte((byte) 0);
                    intoBuf.writeByte((byte) 0);
                    intoBuf.writeByte((byte) 0);
                } else {
                    intoBuf.writeByte((byte) sessionCalendar
                            .get(Calendar.HOUR_OF_DAY));
                    intoBuf.writeByte((byte) sessionCalendar
                            .get(Calendar.MINUTE));
                    intoBuf.writeByte((byte) sessionCalendar
                            .get(Calendar.SECOND));
                }
                if (length == 11) {
                    intoBuf.writeLong(((java.sql.Timestamp) dt).getNanos() / 1000);
                }
            } finally {
                sessionCalendar.setTime(oldTime);
            }
        }
    }

    //存储 时分秒时间
    private void storeTime(SqlBuffer intoBuf, Time tm) throws SQLException {
        intoBuf.ensureCapacity(9);
        intoBuf.writeByte((byte) 8); // 时分秒的长度
        intoBuf.writeByte((byte) 0); // 没有标识
        intoBuf.writeLong(0);
        Calendar sessionCalendar = new GregorianCalendar();
        synchronized (sessionCalendar) {
            java.util.Date oldTime = sessionCalendar.getTime();
            try {
                sessionCalendar.setTime(tm);
                intoBuf.writeByte((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY));
                intoBuf.writeByte((byte) sessionCalendar.get(Calendar.MINUTE));
                intoBuf.writeByte((byte) sessionCalendar.get(Calendar.SECOND));
            } finally {
                sessionCalendar.setTime(oldTime);
            }
        }
    }

    /**
     * 关闭当前预编译语句
     * @throws SQLException
     */
    public void close() throws SQLException{
        if (isCached && !this.isClosed){
            this.connection.reCachePreparedStatement(this);
            return;
        }
        realClose(true);
    }


    protected  void setClosed(boolean flag) {
        this.isClosed = flag;
    }

    /**
     * 关闭当前预编译
     * @param closeOpenResults 是否关闭resultSet
     */
    public void realClose(boolean closeOpenResults) throws SQLException {
        if (isClosed){
            return;
        }
        if (this.connection !=null && this.connection.isClosed()){
            try {
                synchronized (this.connection) {
                    MysqlIO mysqlIO = this.connection.getIO();
                    SqlBuffer sqlBuffer = new SqlBuffer(9);
                    sqlBuffer.writeByte((byte) MysqlDefs.COM_STMT_CLOSE);
                    sqlBuffer.writeLong(this.serverStatementId);
                    mysqlIO.sendCommand(MysqlDefs.COM_STMT_CLOSE,null,0,sqlBuffer,true);
                }
            }catch (SQLException ex){
                ex.printStackTrace();
            }
        }
        super.realClose(closeOpenResults);
    }

    public static class BindValue {

        //参数长度
        public long bindLength;

        //参数类型
        public int bufferType;

        // 参数double类型绑定值
        public double doubleBinding;

        // 参数float类型绑定值
        public float floatBinding;

        //是否是长数据 Clob，Blob类型
        public boolean isLongData;

        //是否为null
        public boolean isNull;

        //是否已经设置值
        public boolean isSet = false;

        // 参数int,short,byte long类型绑定值
        public long longBinding;

        //参数string,byte[] 类型
        public Object value;

        BindValue() {
        }

        void reset() {
            this.isSet = false;
            this.value = null;
            this.isLongData = false;
            this.longBinding = 0L;
            this.floatBinding = 0;
            this.doubleBinding = 0D;
        }

        public String toString() {
            return toString(false);
        }

        public String toString(boolean quoteIfNeeded) {
            if (this.isLongData) {
                return "' STREAM DATA '";
            }
            switch (this.bufferType) {
                case MysqlDefs.FIELD_TYPE_TINY:
                case MysqlDefs.FIELD_TYPE_SHORT:
                case MysqlDefs.FIELD_TYPE_LONG:
                case MysqlDefs.FIELD_TYPE_LONGLONG:
                    return String.valueOf(longBinding);
                case MysqlDefs.FIELD_TYPE_FLOAT:
                    return String.valueOf(floatBinding);
                case MysqlDefs.FIELD_TYPE_DOUBLE:
                    return String.valueOf(doubleBinding);
                case MysqlDefs.FIELD_TYPE_TIME:
                case MysqlDefs.FIELD_TYPE_DATE:
                case MysqlDefs.FIELD_TYPE_DATETIME:
                case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                case MysqlDefs.FIELD_TYPE_VAR_STRING:
                case MysqlDefs.FIELD_TYPE_STRING:
                case MysqlDefs.FIELD_TYPE_VARCHAR:
                    if (quoteIfNeeded) {
                        return "'" + value + "'";
                    } else {
                        return String.valueOf(value);
                    }
                default:
                    if (value instanceof byte[]) {
                        return "byte data";
                    } else {
                        if (quoteIfNeeded) {
                            return "'" + value + "'";
                        } else {
                            return String.valueOf(value);
                        }
                    }
            }
        }

    }

    /**
     * 设置值的类型,
     * @param oldValue 是否已经有类型
     * @param bufferType 当前类型
     */
    protected synchronized void setType(BindValue oldValue, int bufferType) {
        if (oldValue.bufferType != bufferType) {
            this.sendTypesToServer = true;
        }
        oldValue.bufferType = bufferType;
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_LONG);
        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        if (binding.bufferType == 0) {
            setType(binding, MysqlDefs.FIELD_TYPE_NULL);
        }
        binding.value = null;
        binding.isNull = true;
        binding.isLongData = false;
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setByte(parameterIndex, (x ? (byte) 1 : (byte) 0));
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_TINY);
        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_SHORT);
        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_LONGLONG);
        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_FLOAT);
        binding.value = null;
        binding.floatBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_DOUBLE);
        binding.value = null;
        binding.doubleBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.DECIMAL);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_NEW_DECIMAL);
            binding.value = x.toString();
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.CHAR);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_STRING);
            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_VAR_STRING);
            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex,x,null);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.DATE);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_DATE);
            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }


    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex,x,null);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIME);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_TIME);
            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex,x,null);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_DATETIME);
            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    public void clearParameters() throws SQLException {
        if (this.parameterBindings != null) {
            for (int i = 0; i < this.parameterCount; i++) {
                if ((this.parameterBindings[i] != null)
                        && this.parameterBindings[i].isLongData) {
                }
                this.parameterBindings[i].reset();
            }
        }
    }

    /**
     * 获取参数的bing
     * @param parameterIndex 参数位置
     * @param forLongData 是否是长数据
     * @return
     */
    private BindValue getBinding(int parameterIndex, boolean forLongData) throws SQLException {
        if (this.parameterBindings.length == 0){
            throw new SQLException("没有参数定义");
        }
        int index = parameterIndex -1;
        if (index<0){
            throw new SQLException("parameterIndex不能小于0");
        }
        if (this.parameterBindings[index] == null) {
            this.parameterBindings[index] = new BindValue();
        } else {
            if (this.parameterBindings[index].isLongData && !forLongData) {
                this.detectedLongParameterSwitch = true;
            }
        }
        this.parameterBindings[index].isSet = true;
        return this.parameterBindings[index];
    }
}
