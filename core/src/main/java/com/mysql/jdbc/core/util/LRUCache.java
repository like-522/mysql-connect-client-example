package com.mysql.jdbc.core.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author hjx
 */
public class LRUCache extends LinkedHashMap {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }


    protected boolean removeEldestEntry(Map.Entry eldest) {
        return (size() > this.maxElements);
    }
}
