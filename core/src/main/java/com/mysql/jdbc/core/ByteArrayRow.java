package com.mysql.jdbc.core;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * @author hjx
 */
public class ByteArrayRow extends ResultSetRow{
    byte[][] internalRowData;

    public ByteArrayRow(byte[][] internalRowData){
        this.internalRowData = internalRowData;
    }

    @Override
    public ResultSetRow setMetadata(Field[] metadata) {
        super.metadata =metadata;
        return this;
    }

    @Override
    public byte[] getColumnIndexByte(int columnIndex) {
        return this.internalRowData[columnIndex];
    }

    public boolean isNull(int index){
        return this.internalRowData[index] == null;
    }

    @Override
    public Date getNativeDate(int columnIndex, Calendar cal) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];
        if (columnValue == null) {
            return null;
        }
        return getNativeDate(columnValue,0,columnValue.length,cal);
    }

    @Override
    public Time getNativeTime(int columnIndex, Calendar cal) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];
        if (columnValue == null) {
            return null;
        }
        return getNativeTime(columnValue,0,columnValue.length,cal);
    }

    @Override
    public Timestamp getNativeTimestamp(int columnIndex, Calendar cal) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];
        if (columnValue == null) {
            return null;
        }
        return getNativeTimestamp(columnValue,0,columnValue.length,cal);
    }

    @Override
    public Timestamp getTimestampFast(int columnIndex, Calendar cal) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];
        if (columnValue == null) {
            return null;
        }
        return getTimestampFast(columnIndex,columnValue,0,columnValue.length,cal);
    }

    @Override
    public Time getTimeFast(int columnIndex, Calendar cal) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];
        if (columnValue == null) {
            return null;
        }
        return getTimeFast(columnIndex,columnValue,0,columnValue.length,cal);
    }

    @Override
    public Date getDateFast(int columnIndex, Calendar cal) throws SQLException{
        byte[] columnValue = this.internalRowData[columnIndex];
        if (columnValue == null) {
            return null;
        }
        return getDateFast(columnIndex,columnValue,0,columnValue.length,cal);
    }

    @Override
    public int getNativeInt(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }
        return getNativeInt(this.internalRowData[columnIndex], 0);
    }

    @Override
    public short getNativeShort(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }
        return getNativeShort(this.internalRowData[columnIndex], 0);
    }

    @Override
    public long getNativeLong(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }
        return getNativeLong(this.internalRowData[columnIndex], 0);
    }

    @Override
    public double getNativeDouble(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }
        return getNativeDouble(this.internalRowData[columnIndex], 0);
    }

    @Override
    public float getNativeFloat(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }
        return getNativeFloat(this.internalRowData[columnIndex], 0);
    }
}
