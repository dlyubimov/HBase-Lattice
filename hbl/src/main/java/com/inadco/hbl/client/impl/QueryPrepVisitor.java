/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */
package com.inadco.hbl.client.impl;

import java.util.ArrayDeque;
import java.util.Deque;

import org.antlr.runtime.tree.CommonTree;

import com.inadco.hbl.client.HblException;

/**
 * Query AST tree visitor.
 * 
 * @author dmitriy
 * 
 */
public class QueryPrepVisitor implements QueryVisitor {

    private PreparedAggregateQueryImpl query;
    private Deque<String[]>            selectExpr = new ArrayDeque<String[]>();

    public QueryPrepVisitor(PreparedAggregateQueryImpl query) {
        super();
        this.query = query;
    }

    @Override
    public void reset() {
        selectExpr.clear();
    }

    @Override
    public void visitSelect(CommonTree selectionList,
                            CommonTree fromClause,
                            CommonTree whereClause,
                            CommonTree groupClause) {
        int i = 0;
        for (String[] expr : selectExpr) {
            if (expr.length == 2)
                // id, alias
                query.addAggregateResultDef(i++, expr[0], expr[1]);
            else if (expr.length == 3) {
                // alias,func,measure name
                query.addMeasure(expr[2]);
                query.addAggregateResultDef(i++, expr[0], expr[1], expr[2]);
            }
        }
    }

    @Override
    public void visitGroupDimension(String dim) {
        // DEBUG
        // System.out.printf("Adding group dimension %s.\n", dim);

        query.addGroupBy(dim);
    }

    @Override
    public void visitSlice(String dimension, boolean leftOpen, Object left, boolean rightOpen, Object right) {

        if (right == null)
            // cause nothing else makes sense here
            query.addClosedSlice(dimension, left, left);
        else
            query.addSlice(dimension, left, leftOpen, right, rightOpen);

    }

    @Override
    public void visitSelectExpressionAsID(String id, String alias) {

        selectExpr.add(new String[] { id, alias });
        // query.addAggregateResultDef(selectExprIndex++, alias, id);

    }

    @Override
    public void visitSelectExpressionAsAggrFunc(String func, String measure, String alias) {

        /*
         * can't do it here because cube expression is not set at that point.
         */
        selectExpr.add(new String[] { alias, func, measure });
        // query.addMeasure(measure);
        // query.addAggregateResultDef(selectExprIndex++, alias, func, measure);
    }

    @Override
    public void visitCube(String cubeName) throws HblException {
        query.setCube(cubeName);
    }

}
