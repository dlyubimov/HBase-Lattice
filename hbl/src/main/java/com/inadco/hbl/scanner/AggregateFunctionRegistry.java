package com.inadco.hbl.scanner;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.scanner.functions.FCount;
import com.inadco.hbl.scanner.functions.FSum;

public class AggregateFunctionRegistry {

    private final Map<String, AggregateFunction> functions = new HashMap<String, AggregateFunction>();

    public AggregateFunctionRegistry() {
        super();
        // standard aggregates 
        addFunction(new FCount());
        addFunction(new FSum());
    }

    public void applyAll(Aggregation.Builder accumulator, Double measure ) { 
        for (AggregateFunction af:functions.values()) 
            af.apply(accumulator, measure);
    }
    
    
    
    public void mergeFunctions(Collection<String> funcNames,
                               Aggregation.Builder accumulator,
                               Aggregation source,
                               SliceOperation operation) {

        for (String funcName : funcNames) {
            AggregateFunction func = functions.get(funcName);
            if (func == null)
                throw new UnsupportedOperationException(String.format("Unsupported aggregate function:\"%s\".",
                                                                      funcName));
            func.merge(accumulator, source, operation);
        }

    }
    
    public void mergeAll(Aggregation.Builder accumulator, Aggregation source, SliceOperation operation) { 
        for ( AggregateFunction af:functions.values()) 
            af.merge(accumulator, source, operation);
    }

    public void addFunction(AggregateFunction function) {
        functions.put(function.getName().toUpperCase(), function);
    }

}
