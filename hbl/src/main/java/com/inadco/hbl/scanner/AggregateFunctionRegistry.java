package com.inadco.hbl.scanner;

import java.util.Collection;
import java.util.Map;

import com.inadco.hbl.protocodegen.Cells.Aggregation;

public class AggregateFunctionRegistry {

    private Map<String, AggregateFunction> functions;

    public AggregateFunctionRegistry(Map<String, AggregateFunction> functions) {
        super();
        this.functions = functions;
    }

    public void applyFunctions(Collection<String> funcNames, Aggregation.Builder accumulator, Aggregation source, SliceOperation operation) {

        for (String funcName : funcNames) {
            AggregateFunction func = functions.get(funcName);
            if (func == null)
                throw new UnsupportedOperationException(String.format("Unsupported aggregate function:\"%s\".", funcName));
            func.performMerge(accumulator, source, operation);
        }

    }

}
