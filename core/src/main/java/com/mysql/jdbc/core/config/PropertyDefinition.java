package com.mysql.jdbc.core.config;

import java.util.Properties;

/**
 * @author hjx
 */
public class PropertyDefinition {
    private   String name;
    private  Object value;
    private String description;
    public PropertyDefinition(String name, Object value, String description) {
        this.name = name;
        this.value = value;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    /**
     * 初始化配置的值
     * @param infoCopy
     */
    public void initializeFrom(Properties infoCopy) {
        if (infoCopy.containsKey(name)){
            Object value = infoCopy.remove(name);
            if (value !=null){
                this.value = value;
            }
        }
    }
}
