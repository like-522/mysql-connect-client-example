package com.mysql.jdbc.core;

import com.mysql.jdbc.core.util.StringUtils;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * @author hjx
 */
public class SqlBuffer {

    //字节长度
    private int bufLength;

    //字节数组
    private byte[] byteBuffer;

    //字节位置
    private int position;

    //错误标识,或者没有传数据
    static final long NULL_LENGTH = -1;

    public final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public SqlBuffer(byte[] buf){
        byteBuffer = buf;
        position = 0;
        bufLength = buf.length;
    }

    public SqlBuffer(int length){
        byteBuffer = new byte[length];
        this.position = MysqlIO.HEADER_LENGTH;
        bufLength =length;
    }

    public int getBufLength() {
        return bufLength;
    }
    private void setBufLength(int length) {
        this.bufLength = length;
    }
    public int getPosition() {
        return position;
    }
    public void setPosition(int position) {
        this.position = position;
    }
    public byte[] getByteBuffer() {
        return this.byteBuffer;
    }

    //读取一个字节
    final byte readByte() {
        return this.byteBuffer[this.position++];
    }

    //读取字符串,以null结尾.字节0
    final String readStringWithNull(String encoding) throws SQLException{
        int i = this.position;
        int len = 0;
        int maxLen = getBufLength();
        while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
            len++;
            i++;
        }
        try {
            return StringUtils.toString(this.byteBuffer, this.position, len, encoding);
        }catch (Exception e){
            e.printStackTrace();
            throw new SQLException("read byte error.");
        }finally {
            //最后一个 this.byteBuffer[i] = 0的字节读取了需要 加上去.
            position +=(len+1);
        }
    }

    final byte[] readByteWithNull()throws SQLException{
        int i = this.position;
        int len= 0;
        int maxLen = getBufLength();
        while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
            len++;
            i++;
        }
        try {
            return ByteBuffer.wrap(this.byteBuffer,this.position,len).array();
        }catch (Exception e){
            e.printStackTrace();
            throw new SQLException("read byte error.");
        }finally {
            //最后一个 this.byteBuffer[i] = 0的字节读取了需要 加上去.
            position +=(len+1);
        }
    }

    /**
     * 读取长度的字节数,跳过传入偏移量位置的字节
     * @param offset  偏移量
     * @return
     */
    final byte[] readLenByteArray(int offset) {
        long len = this.readLength();
        if (len == NULL_LENGTH) {
            return null;
        }
        if (len == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        this.position += offset;
        return getBytes((int) len);
    }

    final byte[] getBytes(int len) {
        byte[] b = new byte[len];
        System.arraycopy(this.byteBuffer, this.position, b, 0, len);
        this.position += len; // update cursor
        return b;
    }


    //读取4个字节
    final long readLong() {
        byte[] b = this.byteBuffer;
        return ((long) b[this.position++] & 0xff)
                | (((long) b[this.position++] & 0xff) << 8)
                | ((long) (b[this.position++] & 0xff) << 16)
                | ((long) (b[this.position++] & 0xff) << 24);
    }

    //读取2个字节
    final int readInt() {
        byte[] b = this.byteBuffer;
        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);
    }

    final int readLongInt() {
        byte[] b = this.byteBuffer;
        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8 | ((b[this.position++] & 0xff) << 16));
    }

    final long readLongLong() {
        byte[] b = this.byteBuffer;
        return ((long) b[this.position++] & 0xff)
                | (((long) b[this.position++] & 0xff) << 8)
                | ((long) (b[this.position++] & 0xff) << 16)
                | ((long) (b[this.position++] & 0xff) << 24)
                | ((long) (b[this.position++] & 0xff) << 32)
                | ((long) (b[this.position++] & 0xff) << 40)
                | ((long) (b[this.position++] & 0xff) << 48)
                | ((long) (b[this.position++] & 0xff) << 56);
    }

    final int readLength(){
        int len =  this.readByte() & 0xff;
        switch (len){
            case 251 : return (int) NULL_LENGTH;
            case 252 : return readInt();
            case 253 : return readLongInt();
            case 254 : return (int) readLongLong();
            default: return len;
        }
    }

    final String readStringWithLength(String encoding) {
        int len = readLength();
        byte[] bytes = new byte[len];
        System.arraycopy(this.byteBuffer,this.position,bytes,0,len);
        this.position+=len;
        try {
            return new String(bytes,encoding);
        }catch (UnsupportedEncodingException e){
            return new String(bytes);
        }
    }

    final String readRestOfPacketString(String encoding) {
        int len = this.bufLength - this.position;
        byte[] bytes = new byte[len];
        System.arraycopy(this.byteBuffer,position,bytes,0,len);
        try {
            return new String(bytes,encoding);
        }catch (UnsupportedEncodingException e){
            return new String(bytes);
        }
    }

    //写4个字节
    final void writeLong(long i) throws SQLException {
        ensureCapacity(4);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }



    /**
     * 如果字节数不够,扩容字节.
     * @param additionalData
     * @throws SQLException
     */
    final void ensureCapacity(int additionalData) throws SQLException {
        if ((this.position + additionalData) > getBufLength()) {

            int newLength = (int) (this.byteBuffer.length * 1.25);
            if (newLength < (this.byteBuffer.length + additionalData)) {
                newLength = this.byteBuffer.length + (int) (additionalData * 1.25);
            }
            if (newLength < this.byteBuffer.length) {
                newLength = this.byteBuffer.length + additionalData;
            }
            byte[] newBytes = new byte[newLength];
            System.arraycopy(this.byteBuffer, 0, newBytes, 0,
                    this.byteBuffer.length);
            this.byteBuffer = newBytes;
            setBufLength(this.byteBuffer.length);
        }
    }

    //写一个字节
    final void writeByte(byte b) throws SQLException {
        ensureCapacity(1);
        this.byteBuffer[this.position++] = b;
    }

    /**
     * 写入字节数据
     * @param bytes 写入的字节
     * @throws SQLException
     */
    final void writeBytesNoNull(byte[] bytes) throws SQLException {
        int len = bytes.length;
        ensureCapacity(len);
        System.arraycopy(bytes, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    /**
     *  写入字符串,以null为结尾, 字节0
     * @param str 字符串
     * @param encoding 字符编码
     * @throws SQLException
     */
    final void writeStringWithNull(String str, String encoding) throws SQLException {
        try {
            byte[] bytes = str.getBytes(encoding);
            writeBytesNoNull(bytes);
        }catch (UnsupportedEncodingException e){
            byte[] bytes = str.getBytes();
            writeBytesNoNull(bytes);
        }finally {
            //以0为结尾,代表当前字符串的结束
            this.byteBuffer[this.position++] = 0;
        }
    }


    /**
     * 写入字节,字节的开始字节表示字节的长度
     *
     * @param src 字节
     * @throws SQLException
     */
    final void writeBytesLength(byte[] src) throws SQLException {
        int length = src.length;
        if (length < 251) {
            this.writeByte((byte) length);
          //  0x10000L = 65536 2个字节
        } else if (length < 0x10000L) {
            this.writeByte((byte) 252);
            writeInt(length);
            // 3个字节
        } else if (length < 0x1000000L) {
            this.writeByte((byte) 253);
            writeLongInt(length);
        } else {
            // 8个字节
            this.writeByte((byte) 254);
            writeLongLong(length);
        }
        this.writeBytesNoNull(src);
    }

    //写入两个字节
     final void writeInt(int i) throws SQLException {
        byte[] b = new byte[2];
        b[0]=(byte)(i & 0xff);
        b[1] = (byte)(i >>> 8);
        this.writeBytesNoNull(b);
    }

    //写入3个字节
    final void writeLongInt(int i) throws SQLException {
        ensureCapacity(3);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
    }

    //写入float类型
    final void writeFloat(float f) throws SQLException {
        int i = Float.floatToIntBits(f);
        this.writeLong(i);
    }
    //写入double类型
    final void writeDouble(double d) throws SQLException {
        long l = Double.doubleToLongBits(d);
        this.writeLongLong(l);
    }

    //写8个字节
    final void writeLongLong(long i) throws SQLException {
        byte[] b = new byte[8];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        b[3] = (byte) (i >>> 24);
        b[4] = (byte) (i >>> 32);
        b[5] = (byte) (i >>> 40);
        b[6] = (byte) (i >>> 48);
        b[7] = (byte) (i >>> 56);
        this.writeBytesNoNull(b);
    }

    //EOF响应格式
    final boolean isLastDataPacket() {
        return ((getBufLength() < 9) && ((this.byteBuffer[0] & 0xff) == 254));
    }

    final void clear() {
        this.position = MysqlIO.HEADER_LENGTH;
    }


    public int fastSkipLenString() {
        int len = readLength();
        this.position+=len;
        return len;
    }

}
