package com.inadco.hbl.scanner;

import com.inadco.hbl.api.Cuboid;

public class DefaultKeyOperationStrategy implements KeyOperationStrategy {

    @Override
    public SliceOperation getKeyOperation(byte[] row, Cuboid cuboid) {
        return SliceOperation.ADD;
    }

}
