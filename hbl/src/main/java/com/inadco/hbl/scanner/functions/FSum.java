package com.inadco.hbl.scanner.functions;

import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.protocodegen.Cells.Aggregation.Builder;
import com.inadco.hbl.scanner.AggregateFunction;
import com.inadco.hbl.scanner.SliceOperation;

/**
 * aggregate function of sum
 * 
 * @author dmitriy
 * 
 */
public class FSum implements AggregateFunction {
    
    @Override
    public String getName() {
        return "SUM";
    }
    
    @Override
    public boolean supportsComplementScan() {
        return true;
    }

    @Override
    public void merge(Builder accumulator, Aggregation source, SliceOperation operation) {
        if (!source.hasSum())
            return;

        switch (operation) {
        case ADD:
            accumulator.setSum(accumulator.hasSum() ? accumulator.getSum() + source.getSum() : source.getSum());
            break;
        case COMPLEMENT:
            accumulator.setSum(accumulator.hasSum() ? accumulator.getSum() - source.getSum() : -source.getSum());
        }
    }

    @Override
    public void apply(Builder result, Double measure) {
        if ( measure == null ) return;
        double sum=result.hasSum()?0d:result.getSum();
        sum+=measure;
        result.setSum(sum);
    }
    
    

}
