package com.mysql.jdbc.core;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;

/**
 * @author hjx
 */
public class Field {

    //当前字段的连接
    private MysqlConnection connection;
    byte[] buffer;

    //always "def"
    int catalogLenStart;
    int catalogLength;

    int databaseLenStart;
    int databaseLength;

    int tableNameLenStart;
    int tableNameLength;

    int originalTableNameLenStart;
    int originalTableNameLength;

    int fieldNameLenStart;
    int fieldNameLenLength;

    int originalFieldNameLenStart;
    int originalFieldNameLenLength;

    //字段长度
    long colLength;

    //字段类型
    int colType;

    //字段的功能标识,不为null,是否主键等
    long colFlag;

    //小数标识
    //0x00 for integers and static strings
    //0x1f for dynamic strings, double, float
    //0x00 to 0x51 for decimals
    int decimals;

      //字段的编码
      // 8	0x08	latin1_swedish_ci
     //33	0x21	utf8mb3_general_ci
     //63	0x3f	binary
    int charSetNumber;

    //表名
    String tableName;
    //字段名
    String name;
    //字段长度
    int length;

    public Field(MysqlConnection connection, byte[] byteBuffer, int catalogLenStart, int catalogLength,
                 int databaseLenStart, int databaseLength,
                 int tableNameLenStart, int tableNameLength, int originalTableNameLenStart,
                 int originalTableNameLength, int fieldNameLenStart, int fieldNameLenLength,
                 int originalFieldNameLenStart, int originalFieldNameLenLength,
                 long colLength, int colType, long colFlag, int decimals, int charSetNumber) {
        this.connection = connection;
        this.buffer = byteBuffer;
        this.catalogLenStart = catalogLenStart;
        this.catalogLength = catalogLength;
        this.databaseLenStart = databaseLenStart;
        this.databaseLength= databaseLength;
        this.tableNameLenStart =  tableNameLenStart;
        this.tableNameLength = tableNameLength;
        this.originalTableNameLenStart = originalTableNameLenStart;
        this.originalTableNameLength = originalTableNameLength;
        this.fieldNameLenStart = fieldNameLenStart;
        this.fieldNameLenLength = fieldNameLenLength;
        this.originalFieldNameLenStart = originalFieldNameLenStart;
        this.originalFieldNameLenLength = originalFieldNameLenLength;
        this.colLength = colLength;
        this.colType = colType;
        this.colFlag = colFlag;
        this.decimals =decimals;
        this.charSetNumber = charSetNumber;
    }

    public Field(MysqlConnection connection,String tableName, String columnName, int jdbcType, int length){
        this.connection = connection;
        this.tableName = tableName;
        this.name = columnName;
        this.length = length;
        this.colType = jdbcType;
        this.colFlag = 0;
        this.decimals = 0;
    }

    public int getMysqlType() {
       return colType;
    }

    public String getName() {
        if (this.name == null){
             this.name = getStringFromBytes(fieldNameLenStart,fieldNameLenLength);
        }
        return this.name;
    }

    private String getStringFromBytes(int fieldNameLenStart, int fieldNameLenLength) {
        byte[] nameBytes = ByteBuffer.wrap(this.buffer,fieldNameLenStart,fieldNameLenLength).array();
        return new String(nameBytes);
    }
}
