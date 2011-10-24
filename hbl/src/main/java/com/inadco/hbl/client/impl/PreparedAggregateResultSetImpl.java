package com.inadco.hbl.client.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTablePool;

import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.PreparedAggregateResult;
import com.inadco.hbl.client.impl.scanner.ScanSpec;

/**
 * a result set that can answer requests for values based on prepared query
 * index and expression alias name in the select query.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class PreparedAggregateResultSetImpl extends AggregateResultSetImpl implements PreparedAggregateResult {

    private Map<String, Object>  resultDefByAlias;
    private Map<Integer, Object> resultDefByIndex;

    PreparedAggregateResultSetImpl(List<ScanSpec> scanSpecs,
                           ExecutorService es,
                           HTablePool tpool,
                           AggregateFunctionRegistry afr,
                           Map<String, Integer> measureName2IndexMap,
                           Map<String, Integer> dimName2GroupKeyOffsetMap,
                           Map<Integer, Object> resultDefByIndex,
                           Map<String, Object> resultDefByAlias) throws IOException {
        super(scanSpecs, es, tpool, afr, measureName2IndexMap, dimName2GroupKeyOffsetMap);
        this.resultDefByAlias = resultDefByAlias;
        this.resultDefByIndex = resultDefByIndex;
    }

    @Override
    public Object getObject(String alias) throws HblException {
        Object def = resultDefByAlias.get(alias);
        Validate.notNull(def, "Invalid attribute alias requested.");

        return getObject(def);
    }

    @Override
    public Object getObject(int index) throws HblException {
        Object def = resultDefByIndex.get(index);
        Validate.notNull(def, "Invalid attribute index supplied.");

        return getObject(def);
    }

    Object getObject(Object def) throws HblException {
        if (def instanceof String[]) {
            // aggregate measure
            String[] aggrDef = (String[]) def;
            return super.getDoubleAggregate(aggrDef[0], aggrDef[1]);
        }
        // otherwise it is a dimension group member request
        String dim = (String) def;
        return super.getGroupMember(dim);

    }

}
