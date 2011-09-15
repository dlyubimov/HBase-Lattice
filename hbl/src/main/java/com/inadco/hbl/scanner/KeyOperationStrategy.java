package com.inadco.hbl.scanner;

import com.inadco.hbl.api.Cuboid;

public interface KeyOperationStrategy {
    SliceOperation getKeyOperation(byte[] row, Cuboid cuboid ); 

}
