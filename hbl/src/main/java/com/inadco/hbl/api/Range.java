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

import org.apache.hadoop.io.Writable;

import com.inadco.hbl.util.HblUtil;

public class Range implements Writable {

    private byte[] start, end;
    private boolean leftOpen, rightOpen;

    @Override
    public void readFields(DataInput in) throws IOException {
        start = new byte[HblUtil.readVarUint32(in)];
        in.readFully(start);
        end = new byte[HblUtil.readVarUint32(in)];
        in.readFully(end);
        int stuff = in.readByte();
        if (stuff == -1)
            throw new IOException("Unexpected EOF reading range specficiation");
        leftOpen = (stuff & 0x01) != 0;
        rightOpen = (stuff & 0x02) != 0;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        HblUtil.writeVarUint32(out, start.length);
        out.write(start);
        HblUtil.writeVarUint32(out, end.length);
        out.write(end);
        int stuff = 0;
        if (leftOpen)
            stuff |= 0x01;
        if (rightOpen)
            stuff |= 0x02;
        out.writeByte(stuff);
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

}
