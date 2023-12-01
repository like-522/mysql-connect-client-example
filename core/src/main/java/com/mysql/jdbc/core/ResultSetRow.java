package com.mysql.jdbc.core;
import com.mysql.jdbc.core.util.StringUtils;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author hjx
 */
public abstract class ResultSetRow {

    //字段相关信息
    protected Field[] metadata;

    /**
     * 设置当前行的字段元数据信息
     * @param metadata
     * @return
     */
    public abstract ResultSetRow setMetadata(Field[] metadata);

    /**
     * 获取列对应的字节值
     * @param columnIndex 列行数
     * @return 列的字节value
     */
    public abstract byte[] getColumnIndexByte(int columnIndex);

    /**
     * 判断当前行的列值是否位null
     * @param index
     * @return
     */
    public abstract boolean isNull(int index);

    /**
     * 获取日期
     * @param index
     * @param cal
     * @return
     */
    public abstract Date getNativeDate(int index,Calendar cal) throws SQLException;

    /**
     * 获取时间
     * @param index
     * @param cal
     * @return
     * @throws SQLException
     */
    public abstract Time getNativeTime(int index, Calendar cal) throws SQLException;

    /**
     * 获取年月日,时分秒
     * @param index
     * @param cal
     * @return
     * @throws SQLException
     */
    public abstract Timestamp getNativeTimestamp(int index, Calendar cal) throws SQLException;

    /**
     *  获取年月日,时分秒, 文本类型协议
     * @param index
     * @param cal
     * @return
     */
    public abstract Timestamp getTimestampFast(int index, Calendar cal) throws SQLException;

    /**
     * 获取时分秒,文本类型协议
     * @param index
     * @param cal
     * @return
     * @throws SQLException
     */
    public abstract Time getTimeFast(int index, Calendar cal) throws SQLException;
    /**
     * 获取日期，文本类型
     * @param index
     * @param cal
     * @return
     */
    public abstract Date getDateFast(int index, Calendar cal) throws SQLException;
    protected java.sql.Date getDateFast(int columnIndex, byte[] bits, int offset, int length,Calendar dateCal) throws SQLException {
        int year = 0;
        int month = 0;
        int day = 0;
        Field timeColField = this.metadata[columnIndex];
        if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_TIMESTAMP){
            switch (length) {
                case 29:
                case 21:
                case 19: {
                    year = StringUtils.getInt(bits, offset + 0, offset + 4);
                    month = StringUtils.getInt(bits, offset + 5, offset + 7);
                    day = StringUtils.getInt(bits, offset + 8, offset + 10);
                }
                break;
                case 14:
                case 8: {
                    year = StringUtils.getInt(bits, offset + 0, offset + 4);
                    month = StringUtils.getInt(bits, offset + 4, offset + 6);
                    day = StringUtils.getInt(bits, offset + 6, offset + 8);
                }
                break;
                case 12:
                case 10:
                case 6: {
                    year = StringUtils.getInt(bits, offset + 0, offset + 2);
                    if (year <= 69) {
                        year = year + 100;
                    }
                    month = StringUtils.getInt(bits, offset + 2, offset + 4);
                    day = StringUtils.getInt(bits, offset + 4,
                            offset + 6);
                }
                break;
                case 4: {
                    year = StringUtils.getInt(bits, offset + 0, offset + 4);
                    if (year <= 69) {
                        year = year + 100;
                    }
                    month = StringUtils.getInt(bits, offset + 2, offset + 4);
                }
                break;
                case 2: {
                    year = StringUtils.getInt(bits, offset + 0, offset + 2);
                    if (year <= 69) {
                        year = year + 100;
                    }
                }
                break;
                default:
                    throw new SQLException("getTimeFast err.");
            }
        }else if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_TIME){
            year =  1970;
            month=1;
            day=1;
        }else {
            year = StringUtils.getInt(bits, offset + 0,
                    offset + 4);
            month = StringUtils.getInt(bits, offset + 5,
                    offset + 7);
            day = StringUtils.getInt(bits, offset + 8,
                    offset + 10);
        }
        if (dateCal==null){
            dateCal =  new GregorianCalendar();
        }
        dateCal.clear();
        dateCal.set(year, month-1, day, 0, 0, 0);
        dateCal.set(Calendar.MILLISECOND, 0);
        return new Date(dateCal.getTimeInMillis());
    }

    protected java.sql.Time getTimeFast(int columnIndex, byte[] bits, int offset, int length,Calendar dateCal) throws SQLException {
        int hr = 0;
        int min = 0;
        int sec = 0;
        Field timeColField = this.metadata[columnIndex];
        if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_TIMESTAMP) {
            switch (length) {
                case 19: { // YYYY-MM-DD hh:mm:ss
                    hr = StringUtils.getInt(bits, offset + length - 8, offset + length - 6);
                    min = StringUtils.getInt(bits, offset + length - 5, offset + length - 3);
                    sec = StringUtils.getInt(bits, offset + length - 2, offset + length);
                }
                break;
                case 14:
                case 12: {
                    hr = StringUtils.getInt(bits, offset + length - 6, offset + length - 4);
                    min = StringUtils.getInt(bits, offset + length - 4, offset + length - 2);
                    sec = StringUtils.getInt(bits, offset + length - 2, offset + length);
                }
                break;
                case 10: {
                    hr = StringUtils.getInt(bits, offset + 6, offset + 8);
                    min = StringUtils.getInt(bits, offset + 8, offset + 10);
                    sec = 0;
                }
                break;
                default:throw new SQLException("getTimeFast err.");

            }

        } else if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_DATETIME) {
            hr = StringUtils.getInt(bits, offset + 11, offset + 13);
            min = StringUtils.getInt(bits, offset + 14, offset + 16);
            sec = StringUtils.getInt(bits, offset + 17, offset + 19);
        } else {
            hr = StringUtils.getInt(bits, offset + 0, offset + 2);
            min = StringUtils.getInt(bits, offset + 3, offset + 5);
            sec = (length == 5) ? 0 : StringUtils.getInt(bits, offset + 6, offset + 8);
        }
        if (dateCal==null){
            dateCal =  new GregorianCalendar();
        }
        dateCal.clear();
        dateCal.set(1970, 0, 1, hr, min, sec);
        dateCal.set(Calendar.MILLISECOND, 0);
        return new Time(dateCal.getTimeInMillis());
    }

    protected java.sql.Timestamp getTimestampFast(int columnIndex, byte[] bits, int offset, int length,Calendar dateCal) throws SQLException {
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int nanos = 0;
        switch (length) {
            case 29:
            case 26:
            case 25:
            case 24:
            case 23:
            case 22:
            case 21:
            case 20:
            case 19: {
                year = StringUtils.getInt(bits, offset + 0, offset + 4);
                month = StringUtils.getInt(bits, offset + 5, offset + 7);
                day = StringUtils.getInt(bits, offset + 8, offset + 10);
                hour = StringUtils.getInt(bits, offset + 11, offset + 13);
                minutes = StringUtils.getInt(bits, offset + 14, offset + 16);
                seconds = StringUtils.getInt(bits, offset + 17, offset + 19);
                nanos = 0;
                if (length > 19) {
                    int decimalIndex = -1;
                    for (int i = 0; i < length; i++) {
                        if (bits[offset + i] == '.') {
                            decimalIndex = i;
                        }
                    }
                    if (decimalIndex != -1) {
                        if ((decimalIndex + 2) <= length) {
                            nanos = StringUtils.getInt(
                                    bits, offset + decimalIndex + 1,
                                    offset + length);

                            int numDigits = (length) - (decimalIndex + 1);

                            if (numDigits < 9) {
                                int factor = (int) (Math.pow(10, 9 - numDigits));
                                nanos = nanos * factor;
                            }
                        } else {
                            throw new IllegalArgumentException(); // re-thrown
                        }
                    }
                }
                break;
            }
            case 14: {
                year = StringUtils.getInt(bits, offset + 0, offset + 4);
                month = StringUtils.getInt(bits, offset + 4, offset + 6);
                day = StringUtils.getInt(bits, offset + 6, offset + 8);
                hour = StringUtils.getInt(bits, offset + 8, offset + 10);
                minutes = StringUtils.getInt(bits, offset + 10, offset + 12);
                seconds = StringUtils.getInt(bits, offset + 12, offset + 14);
                break;
            }

            case 12: {
                year = StringUtils.getInt(bits, offset + 0, offset + 2);

                if (year <= 69) {
                    year = (year + 100);
                }

                year += 1900;

                month = StringUtils.getInt(bits, offset + 2, offset + 4);
                day = StringUtils.getInt(bits, offset + 4, offset + 6);
                hour = StringUtils.getInt(bits, offset + 6, offset + 8);
                minutes = StringUtils.getInt(bits, offset + 8, offset + 10);
                seconds = StringUtils.getInt(bits, offset + 10, offset + 12);

                break;
            }
            case 10: {
                boolean hasDash = false;

                for (int i = 0; i < length; i++) {
                    if (bits[offset + i] == '-') {
                        hasDash = true;
                        break;
                    }
                }
                if ((this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_DATE)
                        || hasDash) {
                    year = StringUtils.getInt(bits, offset + 0, offset + 4);
                    month = StringUtils.getInt(bits, offset + 5, offset + 7);
                    day = StringUtils.getInt(bits, offset + 8, offset + 10);
                    hour = 0;
                    minutes = 0;
                } else {
                    year = StringUtils.getInt(bits, offset + 0, offset + 2);

                    if (year <= 69) {
                        year = (year + 100);
                    }
                    month = StringUtils.getInt(bits, offset + 2, offset + 4);
                    day = StringUtils.getInt(bits, offset + 4, offset + 6);
                    hour = StringUtils.getInt(bits, offset + 6, offset + 8);
                    minutes = StringUtils.getInt(bits, offset + 8, offset + 10);
                    year += 1900;
                }
                break;
            }
            case 8: {
                boolean hasColon = false;

                for (int i = 0; i < length; i++) {
                    if (bits[offset + i] == ':') {
                        hasColon = true;
                        break;
                    }
                }
                if (hasColon) {
                    hour = StringUtils.getInt(bits, offset + 0, offset + 2);
                    minutes = StringUtils.getInt(bits, offset + 3, offset + 5);
                    seconds = StringUtils.getInt(bits, offset + 6, offset + 8);
                    year = 1970;
                    month = 1;
                    day = 1;
                    break;
                }

                year = StringUtils.getInt(bits, offset + 0, offset + 4);
                month = StringUtils.getInt(bits, offset + 4, offset + 6);
                day = StringUtils.getInt(bits, offset + 6, offset + 8);
                year -= 1900;
                month--;

                break;
            }
            case 6: {
                year = StringUtils.getInt(bits, offset + 0, offset + 2);
                if (year <= 69) {
                    year = (year + 100);
                }
                year += 1900;
                month = StringUtils.getInt(bits, offset + 2, offset + 4);
                day = StringUtils.getInt(bits, offset + 4, offset + 6);
                break;
            }

            case 4: {
                year = StringUtils.getInt(bits, offset + 0, offset + 2);
                if (year <= 69) {
                    year = (year + 100);
                }
                month = StringUtils.getInt(bits, offset + 2, offset + 4);
                day = 1;
                break;
            }
            case 2: {
                year = StringUtils.getInt(bits, offset + 0, offset + 2);
                if (year <= 69) {
                    year = (year + 100);
                }
                year += 1900;
                month = 1;
                day = 1;
                break;
            }
        }
        if (dateCal==null){
            dateCal =  new GregorianCalendar();
        }
        dateCal.clear();
        dateCal.set(year, month-1, day, hour, minutes, seconds);
        dateCal.set(Calendar.MILLISECOND, nanos);
        return new Timestamp(dateCal.getTimeInMillis());
    }


    protected java.sql.Date getNativeDate(byte[] bits, int offset, int length,Calendar dateCal) throws SQLException {
        int year = 0;
        int month = 0;
        int day = 0;
        if (length != 0) {
            year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);

            month = bits[offset + 2];
            day = bits[offset + 3];
        }
        if (length == 0 || ((year == 0) && (month == 0) && (day == 0))) {
            return null;
        }
        if (dateCal==null){
            dateCal =  new GregorianCalendar();
        }
        dateCal.clear();
        dateCal.set(year, month-1, day, 0, 0, 0);
        dateCal.set(Calendar.MILLISECOND, 0);
        return new Date(dateCal.getTimeInMillis());
    }

    protected Time getNativeTime(byte[] bits, int offset, int length, Calendar timeCal) throws SQLException {
        int hour = 0;
        int minute = 0;
        int seconds = 0;
        if (length != 0) {
            hour = bits[offset + 5];
            minute = bits[offset + 6];
            seconds = bits[offset + 7];
        }
        if (timeCal==null){
            timeCal =  new GregorianCalendar();
        }
        timeCal.clear();
        timeCal.set(1970, 0, 1, hour, minute, seconds);
        timeCal.set(Calendar.MILLISECOND, 0);
        return new Time(timeCal.getTimeInMillis());
    }

    protected Timestamp getNativeTimestamp(byte[] bits, int offset, int length,
                                           Calendar timestampCalendar) throws SQLException {
        int year = 0;
        int month = 0;
        int day = 0;

        int hour = 0;
        int minute = 0;
        int seconds = 0;

        int nanos = 0;

        if (length != 0) {
            char value = (char) (bits[0] & 0xff);
            year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);
            month = bits[offset + 2];
            day = bits[offset + 3];

            if (length > 4) {
                hour = bits[offset + 4];
                minute = bits[offset + 5];
                seconds = bits[offset + 6];
            }

            if (length > 7) {
                // MySQL uses microseconds
                nanos = ((bits[offset + 7] & 0xff)
                        | ((bits[offset + 8] & 0xff) << 8)
                        | ((bits[offset + 9] & 0xff) << 16) | ((bits[offset + 10] & 0xff) << 24)) * 1000;
            }
        }
        if (timestampCalendar==null){
            timestampCalendar =  new GregorianCalendar();
        }
        timestampCalendar.clear();
        timestampCalendar.set(year, month-1, day, hour, minute, seconds);
        timestampCalendar.set(Calendar.MILLISECOND, nanos);
        return new Timestamp(timestampCalendar.getTimeInMillis());
    }

    public abstract int getNativeInt(int columnIndex);

    protected int getNativeInt(byte[] bits,int offset){
        int valueAsInt = (bits[offset + 0] & 0xff)
                | ((bits[offset + 1] & 0xff) << 8)
                | ((bits[offset + 2] & 0xff) << 16)
                | ((bits[offset + 3] & 0xff) << 24);
        return valueAsInt;
    }

    public abstract short getNativeShort(int columnIndex);

    protected short getNativeShort(byte[] bits,int offset){
        return (short) ((bits[offset + 0] & 0xff)
                        | ((bits[offset + 1] & 0xff) << 8));
    }

    public abstract long getNativeLong(int columnIndex);

    protected long getNativeLong(byte[] bits,int offset){
        long valueAsLong = (bits[offset + 0] & 0xff)
                | ((bits[offset + 1] & 0xff) << 8)
                | ((bits[offset + 2] & 0xff) << 16)
                | ((bits[offset + 3] & 0xff) << 24)
                | ((bits[offset + 3] & 0xff) << 32)
                | ((bits[offset + 3] & 0xff) << 40)
                | ((bits[offset + 3] & 0xff) << 48)
                | ((bits[offset + 3] & 0xff) << 56);
        return valueAsLong;
    }

    public abstract double getNativeDouble(int columnIndex);

    protected double getNativeDouble(byte[] bits,int offset){
        long valueAsLong = (bits[offset + 0] & 0xff)
                | ((long) (bits[offset + 1] & 0xff) << 8)
                | ((long) (bits[offset + 2] & 0xff) << 16)
                | ((long) (bits[offset + 3] & 0xff) << 24)
                | ((long) (bits[offset + 4] & 0xff) << 32)
                | ((long) (bits[offset + 5] & 0xff) << 40)
                | ((long) (bits[offset + 6] & 0xff) << 48)
                | ((long) (bits[offset + 7] & 0xff) << 56);
        return Double.longBitsToDouble(valueAsLong);
    }

    public abstract float getNativeFloat(int columnIndex);

    protected float getNativeFloat(byte[] bits,int offset){
        int asInt = (bits[offset + 0] & 0xff)
                | ((bits[offset + 1] & 0xff) << 8)
                | ((bits[offset + 2] & 0xff) << 16)
                | ((bits[offset + 3] & 0xff) << 24);
        return Float.intBitsToFloat(asInt);
    }
}
