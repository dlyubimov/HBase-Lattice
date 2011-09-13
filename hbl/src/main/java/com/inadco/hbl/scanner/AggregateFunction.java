package com.inadco.hbl.scanner;

import com.inadco.hbl.protocodegen.Cells.Aggregation;

public interface AggregateFunction {
    
   void apply(Aggregation.Builder result, Double measure ); 
   
   void merge(Aggregation.Builder accumulator, Aggregation source, SliceOperation operation );
   public String getName() ;
   public boolean supportsComplementScan();

}
