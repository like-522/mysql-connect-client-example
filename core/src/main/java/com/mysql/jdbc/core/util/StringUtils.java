package com.mysql.jdbc.core.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hjx
 */
public class StringUtils {

    private static final String platformEncoding = System.getProperty("file.encoding");

    private static final ConcurrentHashMap<String,Charset> charsetsByAlias =
            new ConcurrentHashMap<String,Charset>();
    /**
     * 寻找支持的字符编码
     * @param alias
     * @return
     * @throws UnsupportedEncodingException
     */
    static Charset findCharset(String alias) throws UnsupportedEncodingException {
        try {
            Charset cs = charsetsByAlias.get(alias);
            if (cs == null) {
                cs = Charset.forName(alias);
                charsetsByAlias.putIfAbsent(alias, cs);
            }
            return cs;
        } catch (IllegalArgumentException uce) {
            throw new UnsupportedEncodingException(alias);
        }
    }

    /**
     * 确定字符串 sourceStr 是否包含 searchFor,忽略大小写
     * @param sourceStr 源字符串
     * @param searchStr 搜索的字符串
     * @return 搜索的字符串在源字符串中返回true 否则 false
     */
    public static boolean startsWithIgnoreCase(String sourceStr, String searchStr) {
        return startsWithIgnoreCase(sourceStr, 0, searchStr);
    }

    /**
     *   确定字符串 searchIn 是否包含 searchFor,忽略大小写
     * @param sourceStr  源字符串
     * @param startAt 起始位置
     * @param searchStr 搜索的字符串
     * @return 搜索的字符串在源字符串中返回true 否则 false
     */
    public static boolean startsWithIgnoreCase(String sourceStr, int startAt,
                                               String searchStr) {
        return sourceStr.regionMatches(true, startAt, searchStr, 0, searchStr
                .length());
    }

    /**
     * 将对应字节转换为对应编码的字符串
     * @param value 字节
     * @param offset 起始位置
     * @param length 结束位置
     * @param encoding 编码
     * @return 字符串
     * @throws UnsupportedEncodingException
     */
    public static String toString(byte[] value, int offset, int length,
                                  String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);
        return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
    }

    /**
     * 确定字符串 searchIn 是否包含 searchFor,不考虑大小写、前导空格和非字母数字字符。
     * @param sourceStr  源字符串
     * @param searchStr 搜索的字符串
     * @return 搜索的字符串在源字符串中返回true 否则 false
     */
    public static boolean startsWithIgnoreCaseAndNonAlphaNumeric(
            String sourceStr, String searchStr) {
        if (sourceStr == null) {
            return searchStr == null;
        }
        int beginPos = 0;
        int inLength = sourceStr.length();
        for (beginPos = 0; beginPos < inLength; beginPos++) {
            char c = sourceStr.charAt(beginPos);
            if (Character.isLetterOrDigit(c)) {
                break;
            }
        }
        return startsWithIgnoreCase(sourceStr, beginPos, searchStr);
    }

    /**
     * 确定字符串 searchIn 是否包含 searchFor,不考虑大小写、前导空格
     * @param sourceStr 源字符串
     * @param searchStr 搜索的字符串
     * @return  搜索的字符串在源字符串中返回true 否则 false
     */
    public static boolean startsWithIgnoreCaseAndWs(String sourceStr,
                                                    String searchStr) {
        return startsWithIgnoreCaseAndWs(sourceStr, searchStr, 0);
    }

    /**
     *  确定字符串 searchIn 是否包含 searchFor,不考虑大小写、前导空格
     * @param sourceStr
     * @param searchStr
     * @param beginPos
     * @return
     */
    public static boolean startsWithIgnoreCaseAndWs(String sourceStr,
                                                    String searchStr, int beginPos) {
        if (sourceStr == null) {
            return searchStr == null;
        }
        int inLength = sourceStr.length();
        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(sourceStr.charAt(beginPos))) {
                break;
            }
        }
        return startsWithIgnoreCase(sourceStr, beginPos, searchStr);
    }

    public static byte[] getBytes(String value) {
        try {
            Charset cs = findCharset(platformEncoding);
            ByteBuffer buf = cs.encode(value);
            int encodedLen = buf.limit();
            byte[] asBytes = new byte[encodedLen];
            buf.get(asBytes, 0, encodedLen);
            return asBytes;
        } catch (UnsupportedEncodingException e) {
        }

        return null;
    }
    public static int getInt(byte[] buf, int offset, int endPos) throws NumberFormatException {
        //简单处理
        int base = 10;
        int i = 0;
        for (;offset<endPos;offset++){
            char c = (char) buf[offset];
            if (Character.isDigit(c)) {
                c -= '0';
            } else if (Character.isLetter(c)) {
                c = (char) (Character.toUpperCase(c) - 'A' + 10);
            } else {
                break;
            }
            if (c >=10){
                break;
            }
            i *= base;
            i += c;
        }
        return i;
    }

    /**
     * 字节转为str
     * @param value
     * @return
     */
    public static String toString(byte[] value) {
        try {
            Charset cs = findCharset(platformEncoding);

            return cs.decode(ByteBuffer.wrap(value)).toString();
        } catch (UnsupportedEncodingException e) {
        }

        return null;
    }
}
