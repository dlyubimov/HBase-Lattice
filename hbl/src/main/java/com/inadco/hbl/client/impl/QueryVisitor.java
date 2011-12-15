package com.inadco.hbl.client.impl;

import org.antlr.runtime.tree.CommonTree;

import com.inadco.hbl.client.HblException;

public interface QueryVisitor {

    void reset();

    void visitSelect(CommonTree selectionList, CommonTree fromClause, CommonTree whereClause, CommonTree groupClause);

    void visitSelectExpressionAsID(String id, String alias);

    void visitSelectExpressionAsAggrFunc(String func, String measure, String alias);

    void visitGroupDimension(String dim);

    void visitSlice(String dim, boolean leftOpen, Object left, boolean rightOpen, Object right);

    void visitCube(String cubeName) throws HblException;

}
