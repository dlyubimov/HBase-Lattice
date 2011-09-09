package com.inadco.hbl.scanner;

import com.inadco.hbl.protocodegen.Cells.Aggregation;

public interface AggregateFunction {
    
   void performMerge(Aggregation.Builder accumulator, Aggregation source, SliceOperation operation );

}
