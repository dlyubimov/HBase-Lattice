package com.inadco.hbl.api;

import org.apache.hadoop.io.Writable;

/**
 * Operational bridge and definition holder of measures in the input.
 * 
 * @author dmitriy
 *
 */
public interface Measure  {
    String getName();

    Double asDouble(Object value);

}
