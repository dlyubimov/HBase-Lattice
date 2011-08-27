package com.inadco.hbl.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Range {

    private byte[]  start, end;
    private boolean range;

    public Range(byte[] start, byte[] end, boolean shallow) {
        super();
        range=true;
        this.start=shallow?start:start.clone();
        this.end=shallow?end:end.clone();
    }

    public Range(byte[] singleton) {
        this(singleton, false);
    }

    public Range(byte[] singleton, boolean shallow) {
        super();
        start = shallow ? singleton : singleton.clone();
    }

    public void readFields(DataInput in) throws IOException {

    }

    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

    }

    public boolean isRange() {
        return range;
    }

    public byte[] getStart() {
        return start;
    }

    public byte[] getEnd() {
        return end;
    }


}
