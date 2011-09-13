package com.inadco.hbl.scanner.functions;

import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.protocodegen.Cells.Aggregation.Builder;
import com.inadco.hbl.scanner.AggregateFunction;
import com.inadco.hbl.scanner.SliceOperation;

public class FCount implements AggregateFunction {
    
    
    
    @Override
    public void apply(Builder result, Double measure) {
        if ( measure == null)  return;
        long cnt=result.hasCnt()?result.getCnt():0;
        result.setCnt(cnt+1);
    }


    @Override
    public String getName() {
        return "COUNT";
    }
    

    @Override
    public boolean supportsComplementScan() {
        return true;
    }


    @Override
    public void merge(Builder accumulator, Aggregation source, SliceOperation operation) {
        if (!source.hasCnt())
            return;

        switch (operation) {
        case ADD:
            accumulator.setCnt(accumulator.hasCnt() ? accumulator.getCnt() + source.getCnt() : source.getCnt());
            break;
        case COMPLEMENT:
            accumulator.setCnt(accumulator.hasCnt() ? accumulator.getCnt() - source.getCnt() : -source.getCnt());
            break;
        }

    }

}
