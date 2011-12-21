/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */
package com.inadco.hbl.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.util.HblUtil;

/**
 * element of specification for composite key scan filter.
 * 
 * @author dmitriy
 * 
 */

public class Range implements Writable {

    private byte[]                   start, end;
    private boolean                  leftOpen, rightOpen;
    private int                      subkeyLen;
    private int                      keyLen;

    private transient SliceOperation sliceOperation;

    @Override
    public void readFields(DataInput in) throws IOException {
        keyLen = HblUtil.readVarUint32(in);
        start = new byte[keyLen];
        in.readFully(start);
        end = new byte[keyLen];
        in.readFully(end);
        int stuff = in.readByte();
        if (stuff == -1)
            throw new IOException("Unexpected EOF reading range specficiation");
        leftOpen = (stuff & 0x01) != 0;
        rightOpen = (stuff & 0x02) != 0;
        subkeyLen = HblUtil.readVarUint32(in);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        HblUtil.writeVarUint32(out, keyLen);
        out.write(start);
        out.write(end);
        int stuff = 0;
        if (leftOpen)
            stuff |= 0x01;
        if (rightOpen)
            stuff |= 0x02;
        out.writeByte(stuff);
        HblUtil.writeVarUint32(out, subkeyLen);
    }

    public Range() {
        super();
    }

    public Range(byte[] point) {
        this(point, false);
    }

    /**
     * creates a range with just one point in
     * 
     * @param point
     */
    public Range(byte[] point, boolean shallow) {
        this(point, point, shallow, false, false);
    }

    /**
     * By default, creates a deep copy of the range with a half-open semantics
     */
    public Range(byte[] start, byte[] end) {
        this(start, end, false);
    }

    /**
     * creates a range with half-open interval semantics by default
     * 
     * @param start
     * @param end
     * @param shallow
     */
    public Range(byte[] start, byte[] end, boolean shallow) {
        this(start, end, shallow, false, true);
    }

    public Range(byte[] start, byte[] end, boolean shallow, boolean leftOpen, boolean rightOpen) {
        super();

        Validate.notNull(start);
        Validate.notNull(end);
        Validate.isTrue(Bytes.BYTES_RAWCOMPARATOR.compare(start, end) <= 0, "scan start after scan end");

        this.keyLen = start.length;
        Validate.isTrue(end.length == keyLen);

        this.start = shallow ? start : start.clone();
        this.end = shallow ? end : end.clone();
        this.leftOpen = leftOpen;
        this.rightOpen = rightOpen;
    }

    public byte[] getLeftBound() {
        return start;
    }

    public byte[] getRightBound() {
        return end;
    }

    /**
     * returns true if the range represents a left-open interval; false if
     * left-closed one.
     * 
     * @return
     */
    public boolean isLeftOpen() {
        return leftOpen;
    }

    public void setLeftOpen(boolean leftOpen) {
        this.leftOpen = leftOpen;
    }

    /**
     * returns true if the range represents a right-open interval; false if
     * right-closed one.
     * 
     * @return
     */
    public boolean isRightOpen() {
        return rightOpen;
    }

    public void setRightOpen(boolean rightOpen) {
        this.rightOpen = rightOpen;
    }

    public int getKeyLen() {
        return keyLen;
    }

    public void setKeyLen(int compositeKeyLen) {
        this.keyLen = compositeKeyLen;
    }

    public int getSubkeyLen() {
        return subkeyLen;
    }

    public void setSubkeyLen(int subkeyLen) {
        this.subkeyLen = subkeyLen;
    }

    public SliceOperation getSliceOperation() {
        return sliceOperation;
    }

    public void setSliceOperation(SliceOperation sliceOperation) {
        this.sliceOperation = sliceOperation;
    }

}
