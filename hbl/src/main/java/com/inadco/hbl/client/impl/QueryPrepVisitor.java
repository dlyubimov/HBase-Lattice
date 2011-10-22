package com.inadco.hbl.client.impl;

import org.antlr.runtime.tree.CommonTree;

import com.inadco.hbl.client.PreparedAggregateQuery;

public class QueryPrepVisitor implements QueryVisitor {

    private PreparedAggregateQuery query;

    public QueryPrepVisitor(PreparedAggregateQuery query) {
        super();
        this.query = query;
    }

    @Override
    public void visitSelect(CommonTree selectionList,
                            CommonTree fromClause,
                            CommonTree whereClause,
                            CommonTree groupClause) {

    }

    @Override
    public void visitMeasure(String measure) {

        // DEBUG
        System.out.printf("adding measure %s.\n", measure);

        query.addMeasure(measure);
    }

    @Override
    public void visitDim(String dim) {
        // DEBUG
        System.out.printf("Dimension %s will be requested\n", dim);
    }

    @Override
    public void visitGroupDimension(String dim) {
        System.out.printf("Adding group dimension %s.\n", dim);

        query.addGroupBy(dim);
    }

    @Override
    public void visitSlice(String dimension, boolean leftOpen, Object left, boolean rightOpen, Object right) {

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
}
