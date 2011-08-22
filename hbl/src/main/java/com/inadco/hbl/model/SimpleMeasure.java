package com.inadco.hbl.model;

import com.inadco.hbl.api.Measure;

/**
 * Simple double-measure support 
 * 
 * @author dmitriy
 *
 */
public class SimpleMeasure implements Measure {

    protected String name;
    
    public SimpleMeasure(String name) {
        super();
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Double asDouble(Object value) {
        if (value == null)
            return null;
        else if (value instanceof Double)
            return (Double) value;
        else if (value instanceof Long)
            return ((Long) value).doubleValue();
        else if (value instanceof Integer)
            return ((Integer) value).doubleValue();
        else if (value instanceof Short)
            return ((Short) value).doubleValue();
        else if (value instanceof Byte)
            return ((Byte) value).doubleValue();
        else
            throw new RuntimeException(String.format("Unknown measure instance type: %s", value.getClass().getName()));
    }


}
