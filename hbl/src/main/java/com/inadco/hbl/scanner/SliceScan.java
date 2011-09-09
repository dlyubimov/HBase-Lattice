package com.inadco.hbl.scanner;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

public class SliceScan {

    private Cube           cube;
    private Cuboid         cuboid;
    private Range[]        slicing;
    private SliceOperation operation;

    public SliceScan(Cube cube, Cuboid cuboid, Range[] slicing, SliceOperation operation) {
        super();
        this.cube = cube;
        this.cuboid = cuboid;
        this.slicing = slicing;
        this.operation = operation;
    }

    public Aggregation performScan() {
        // TODO
        return null;
    }

}
