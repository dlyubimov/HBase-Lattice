package com.inadco.hbl.client;

public interface PreparedAggregateQuery extends AggregateQuery {
    
    void prepare ( String statement ) throws HblException;
    void setHblParameter(int param, Object value) throws HblException;

}
