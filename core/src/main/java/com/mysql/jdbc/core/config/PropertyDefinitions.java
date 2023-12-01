package com.mysql.jdbc.core.config;

import java.util.HashMap;
import java.util.Map;

/**
 * @author hjx
 */
public class PropertyDefinitions {

    public static final String DEFAULT_VALUE_TRUE = "true";
    public static final String DEFAULT_VALUE_FALSE = "false";

    public static final Map<String, PropertyDefinition> PROPERTY_KEY_TO_PROPERTY_DEFINITION = new HashMap<>();

    static {
        PropertyDefinition[] pdefs = new PropertyDefinition[] {
                new PropertyDefinition(PropertyKey.cachePreparedStatements.name(),DEFAULT_VALUE_FALSE,""),
                new PropertyDefinition(PropertyKey.useServerPreparedStmts.name(),DEFAULT_VALUE_FALSE,""),
                new PropertyDefinition(PropertyKey.preparedStatementCacheSize.name(),25,""),
                new PropertyDefinition(PropertyKey.preparedStatementCacheSqlLimit.name(),256,""),
                new PropertyDefinition(PropertyKey.socketFactoryClassName.name(),"com.mysql.jdbc.core.StandardSocketFactory",""),
                new PropertyDefinition(PropertyKey.statementInterceptors.name(),null,""),
                new PropertyDefinition(PropertyKey.useUnicode.name(),DEFAULT_VALUE_FALSE,""),
                new PropertyDefinition(PropertyKey.characterEncoding.name(),null,""),
        };
        for (PropertyDefinition pdef : pdefs) {
            PROPERTY_KEY_TO_PROPERTY_DEFINITION.put(pdef.getName(), pdef);
        }
    }

    public static PropertyDefinition getPropertyDefinition(String key) {
        return PROPERTY_KEY_TO_PROPERTY_DEFINITION.get(key);
    }

    public static String getStringPropertyValue(String key){
        return (String) PROPERTY_KEY_TO_PROPERTY_DEFINITION.get(key).getValue();
    }

    public static Boolean getBooleanPropertyValue(String key){
        return Boolean.valueOf(getStringPropertyValue(key));
    }

    public static Integer getIntegerPropertyValue(String key){
        return  (Integer)(PROPERTY_KEY_TO_PROPERTY_DEFINITION.get(key).getValue());
    }

}
