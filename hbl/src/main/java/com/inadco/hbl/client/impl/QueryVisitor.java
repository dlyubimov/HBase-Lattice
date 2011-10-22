package com.inadco.hbl.client.impl;

import org.antlr.runtime.tree.CommonTree;

public interface QueryVisitor {
    
    void visitSelect (CommonTree selectionList, CommonTree fromClause, CommonTree whereClause, CommonTree groupClause);
    void visitMeasure ( String measure );
    void visitDim ( String dim );
    void visitGroupDimension ( String dim);
    void visitSlice ( String dim, boolean leftOpen, Object left, boolean rightOpen, Object right );
    
}
