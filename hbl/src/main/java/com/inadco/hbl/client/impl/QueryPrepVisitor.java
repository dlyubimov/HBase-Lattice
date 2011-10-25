package com.inadco.hbl.client.impl;

import org.antlr.runtime.tree.CommonTree;

public class QueryPrepVisitor implements QueryVisitor {

    private PreparedAggregateQueryImpl query;
    private int selectExprIndex;
    
    public QueryPrepVisitor(PreparedAggregateQueryImpl query) {
        super();
        this.query = query;
    }

    @Override
    public void reset() { 
        selectExprIndex=0;
    }

    @Override
    public void visitSelect(CommonTree selectionList,
                            CommonTree fromClause,
                            CommonTree whereClause,
                            CommonTree groupClause) {

    }


    @Override
    public void visitGroupDimension(String dim) {
        // DEBUG
        System.out.printf("Adding group dimension %s.\n", dim);

        query.addGroupBy(dim);
    }

    @Override
    public void visitSlice(String dimension, boolean leftOpen, Object left, boolean rightOpen, Object right) {

        // DEBUG 
        System.out.printf("Adding slice for %s, left-open:%s, right-open:%s, %s,%s.\n",
                          dimension,
                          leftOpen,
                          rightOpen,
                          left.toString(),
                          right == null ? "" : right.toString());
        if (right == null)
            // cause nothing else makes sense here
            query.addClosedSlice(dimension, left, left);
        else
            query.addSlice(dimension, left, leftOpen, right, rightOpen);

    }

    @Override
    public void visitSelectExpressionAsID(String id, String alias) {
        // DEBUG
        System.out.printf("adding dim member %s aliased as %s.\n", id,alias);
        
        query.addAggregateResultDef(selectExprIndex++, alias, id);
        
    }

    @Override
    public void visitSelectExpressionAsAggrFunc(String func, String measure, String alias) {
        // DEBUG
        System.out.printf("adding aggr func %s for measure %s aliased as %s.\n", func, measure, alias);
        query.addMeasure(measure);
        query.addAggregateResultDef(selectExprIndex++,alias, func, measure);
        
    }

    
    
}
