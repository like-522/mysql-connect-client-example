package com.mysql.jdbc.core;

/**
 * @author hjx
 */
public interface RowData {

    /**
     *  设置字段元数据
     * @param metadata 字段元数据
     */
    void setMetadata(Field[] metadata);

    /**
     * 获取行的数量
     * @return
     */
    int size();

    /**
     * 获取下一行数据
     * @return
     */
    ResultSetRow next();

    /**
     * 是否是第一行之前
     * @return
     */
    boolean isBeforeFirst();

    /**
     * 是否是最 后一行之后
     * @return
     */
    boolean isAfterLast();

    /**
     * 是否是第一行
     * @return
     */
    boolean isFirst();

    /**
     * 是否是最后一行
     * @return
     */
    boolean isLast();

    /**
     * 移动光标到第一行之前
     */
    void beforeFirst();

    /**
     * 移动光标到最后一行
     */
    void afterLast();

    /**
     * 是否为空
     * @return
     */
    boolean isEmpty();

    /**
     * 移动光标到最后一行之前
     */
    void beforeLast();

    /**
     * 获取当前行
     * @return
     */
    int getCurrentRowNumber();
}
