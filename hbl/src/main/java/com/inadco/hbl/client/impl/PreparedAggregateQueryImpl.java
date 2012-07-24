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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTablePool;

import com.inadco.hbl.api.AggregateFunctionRegistry;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.HblQueryClient;
import com.inadco.hbl.client.PreparedAggregateQuery;
import com.inadco.hbl.client.impl.scanner.ScanSpec;
import com.inadco.hbl.hblquery.ErrorAccumulator;
import com.inadco.hbl.hblquery.HBLQueryASTLexer;
import com.inadco.hbl.hblquery.HBLQueryASTParser;
import com.inadco.hbl.hblquery.HBLQueryPrep;

/**
 * Prepared Aggregate Query ( {@link PreparedAggregateQuery} ) implementation .
 * <P>
 * 
 * @author dmitriy
 * 
 */

public class PreparedAggregateQueryImpl extends AggregateQueryImpl implements PreparedAggregateQuery {

    private HBLQueryASTParser    parser            = new HBLQueryASTParser(null);
    private HBLQueryASTLexer     lexer             = new HBLQueryASTLexer();
    private ErrorAccumulator     errors            = new ErrorAccumulator();
    private HBLQueryPrep         prepper           = null;
    private ErrorAccumulator     prepperErrors     = new ErrorAccumulator();
    private QueryPrepVisitor     qVisitor          = new QueryPrepVisitor(this);

    private Tree                 selectAST;
    private Map<Integer, Object> parameters        = new HashMap<Integer, Object>();
    private Map<Integer, Object> resultDefsByIndex = new HashMap<Integer, Object>();
    private Map<String, Object>  resultDefsByAlias = new LinkedHashMap<String, Object>();

    public PreparedAggregateQueryImpl(HblQueryClient client, ExecutorService es, HTablePool tpool) {
        super(client, es, tpool);
        parser.setErrorReporter(errors);
    }

    @Override
    public void prepare(String statement) throws HblException {

        // reset query params, etc.
        reset();
        selectAST = null;

        Validate.notNull(statement);
        lexer.reset();
        lexer.setCharStream(new ANTLRStringStream(statement));
        parser.reset();
        parser.setTokenStream(new CommonTokenStream(lexer));
        try {
            HBLQueryASTParser.select_return r = parser.select();
            selectAST = (Tree) r.getTree();
            // if (parser.getNumberOfSyntaxErrors() > 0)
            if (errors.getErrors().size() > 0) {
                throw new HblException(errors.formatErrors());
            }

        } catch (RecognitionException exc) {
            throw new HblException(exc.getMessage());
        }

    }

    @Override
    public void setHblParameter(int param, Object value) throws HblException {
        parameters.put(param, value);
    }

    protected void reset() {
        super.reset();
        parameters.clear();

        /*
         * this kind of better be part of prepare() step although right now we
         * allow parameterizing measure and dimension names there, so for now
         * unfortunately i have to keep construction of the result set attribute
         * map a part of execute() and reset it here for each new use of the
         * query.
         * 
         * Actually we kind of lent this to the result set, so we just need to
         * create a new one. Of course in reality since the query is prepared,
         * this never has to change... but current AST walker initializes it
         * anyway.
         */

        resultDefsByAlias = new LinkedHashMap<String, Object>();
        resultDefsByIndex = new HashMap<Integer, Object>();

    }
    
    

    void assignASTParams() throws HblException {
        Validate.notNull(selectAST, "statement not prepared");
        if (prepper == null) {
            prepper = new HBLQueryPrep(new CommonTreeNodeStream(selectAST));
            prepper.setHblParams(parameters);
            prepper.setQueryVisitor(qVisitor);
            prepper.setErrorReporter(prepperErrors);
        } else {
            prepper.reset();
            prepper.setTreeNodeStream(new CommonTreeNodeStream(selectAST));
        }

        try {
            prepper.select();
            if (prepperErrors.getErrors().size() > 0)
                throw new HblException(prepperErrors.formatErrors());
        } catch (RecognitionException exc) {
            throw new HblException(exc.getMessage(), exc);
        }

    }
    
    @Override
    public List<ScanSpec> generateScanSpecs(Map<String, Integer> dimName2GroupKeyOffsetMap,
                                            Map<String, Integer> measureName2indexMap) throws IOException, HblException {
        assignASTParams();
        return super.generateScanSpecs(dimName2GroupKeyOffsetMap, measureName2indexMap);
    }

    @Override
    protected AggregateResultSetImpl createResultSet(List<ScanSpec> scanSpecs,
                                                     ExecutorService es,
                                                     HTablePool tpool,
                                                     AggregateFunctionRegistry afr,
                                                     Map<String, Integer> measureName2IndexMap,
                                                     Map<String, Integer> dimName2GroupKeyOffsetMap) throws IOException {
        return new PreparedAggregateResultSetImpl(
            scanSpecs,
            es,
            tpool,
            afr,
            measureName2IndexMap,
            dimName2GroupKeyOffsetMap,
            resultDefsByIndex,
            resultDefsByAlias);
    }

    /**
     * Add measure expression. Measure expression must have an aggregate
     * function, the measure name, and optional alias how to call that
     * expression in the result set.
     * 
     * @param index
     * @param alias
     * @param funcName
     * @param measure
     */
    void addAggregateResultDef(int index, String alias, String funcName, String measure) {

        if (afr.findFunction(funcName) == null)
            throw new IllegalArgumentException(String.format("Unknown function name '%s'.", funcName));
        if (!cube.getMeasures().containsKey(measure))
            throw new IllegalArgumentException(String.format("Unknown measure %s.", measure));
        if (resultDefsByAlias.containsKey(alias))
            throw new IllegalArgumentException(String.format("Alias %s already exists.", alias));

        String[] def = new String[2];
        def[0] = measure;
        def[1] = funcName;
        resultDefsByIndex.put(index, def);
        resultDefsByAlias.put(alias, def);
    }

    /**
     * add dimension expression. Dimension expression can only consist of the
     * dimension name and optional alias. (Optionality is provided by query
     * parser, if it is not specified, then dimension name is used).
     * 
     * @param index
     * @param alias
     * @param dim
     */
    void addAggregateResultDef(int index, String alias, String dim) {
        if (!cube.getDimensions().containsKey(dim))
            throw new IllegalArgumentException(String.format("Unknown dimension %s.", dim));
        if (resultDefsByAlias.containsKey(alias))
            throw new IllegalArgumentException(String.format("Alias %s already exists.", alias));
        resultDefsByIndex.put(index, dim);
        resultDefsByAlias.put(alias, dim);
    }

}
