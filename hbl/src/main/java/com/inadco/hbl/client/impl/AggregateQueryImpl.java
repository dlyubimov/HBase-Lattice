package com.inadco.hbl.client.impl;

import java.io.IOException;

import com.inadco.hbl.client.AggregateQuery;
import com.inadco.hbl.client.AggregateResult;

public class AggregateQueryImpl implements AggregateQuery {

    @Override
    public AggregateQuery addMeasure(String measure) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregateQuery addClosedSlice(String dimension, Object leftBound, Object rightBound) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregateQuery addOpenSlice(String dimension, Object leftBound, Object rightBound) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregateQuery addLeftOpenSlice(String dimension, Object leftBound, Object rightBound) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregateQuery addSlice(String dimension,
                                   Object leftBound,
                                   boolean leftOpen,
                                   Object rightBound,
                                   boolean rightOpen) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregateResult execute() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
