package com.inadco.hbl.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTablePool;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.AggregateResultSet;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.PreparedAggregateQuery;
import com.inadco.hbl.hblquery.HBLQueryASTLexer;
import com.inadco.hbl.hblquery.HBLQueryASTParser;
import com.inadco.hbl.hblquery.HBLQueryPrep;

public class PreparedAggregateQueryImpl extends AggregateQueryImpl implements PreparedAggregateQuery {

    private HBLQueryASTParser parser = new HBLQueryASTParser(null);
    private HBLQueryASTLexer lexer = new HBLQueryASTLexer();
    private HBLQueryPrep prepper = new HBLQueryPrep(null);
    
    private Tree selectAST;
    private Map<Integer,String> parameters = new HashMap<Integer, String>();
    
    public PreparedAggregateQueryImpl(Cube cube, ExecutorService es, HTablePool tpool, AggregateFunctionRegistry afr) {
        super(cube, es, tpool, afr);
        prepper.setAggregateQuery(this);
        prepper.setHblParams(parameters);
    }

    @Override
    public void prepare(String statement) throws HblException {
        Validate.notNull(statement);
        lexer.setCharStream(new ANTLRStringStream(statement));
        parser.setTokenStream(new CommonTokenStream(lexer));
        try { 
            HBLQueryASTParser.select_return r = parser.select();
            selectAST=(Tree)r.getTree();
        } catch ( RecognitionException exc ) { 
            throw new HblException ( exc.getMessage());
        }

    }

    @Override
    public void setHblParameter(int param, String value) throws HblException {
        parameters.put(param, value);
    }

    @Override
    public void reset() {
        super.reset();
        selectAST = null;
        parameters.clear();
    }

    @Override
    public AggregateResultSet execute() throws HblException {

        Validate.notNull(selectAST,"statement not prepared");
        prepper.reset();
        prepper.setTreeNodeStream(new CommonTreeNodeStream(selectAST));
        try { 
            prepper.select();
        } catch ( RecognitionException exc ) { 
            throw new HblException ( exc.getMessage(),exc);
        }
        
        // TODO
        
        return super.execute();
    }
    
    

}
