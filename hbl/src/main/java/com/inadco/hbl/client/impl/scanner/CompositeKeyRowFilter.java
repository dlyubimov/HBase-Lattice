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
package com.inadco.hbl.client.impl.scanner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Range;
import com.inadco.hbl.util.HblUtil;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * filter for ranges of individual parts of a composite key.
 * <P>
 * 
 * Let key be a composite key consisting of individual keys (key1,key2...keyn)
 * concatenated together.
 * <P>
 * 
 * Let's also assume that a range is given for each key<sub>i</sub> as
 * range<sub>i</sub>. Each range corresponds to a mathematical definition of an
 * interval, i.e. it can be (left-, right-) open or closed, and we do not
 * support (left-,right-) unbounded intervals at all.
 * <P>
 * 
 * Then this filter ensures that only rows satisfying individual range scan
 * conditions of each key are getting in.
 * <P>
 * 
 * 
 * @author dmitriy
 * 
 */
public class CompositeKeyRowFilter extends FilterBase {
    private Range[]              pathRange;

    // of course since we are using Writable serialization rather than java
    // native, keyword 'transitve' doesn't mean anything in this context, but
    // i'd like to use it as a marker for something i don't really serialize.
    private transient int[]      subkeyLengths;
    private transient int[]      keyOffsets;
    private transient int        compositeKeyLen;
    private transient byte[]     nextKeyHint;
    private transient ReturnCode nextKeyCode;

    public CompositeKeyRowFilter(Range[] pathRange) throws IOException {
        super();

        Validate.notNull(pathRange);
        this.pathRange = pathRange;

    }

    public CompositeKeyRowFilter() {
        super();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int keyNum = HblUtil.readVarUint32(in);
        pathRange = new Range[keyNum];
        keyOffsets = new int[keyNum];
        subkeyLengths = new int[keyNum];
        for (int i = 0; i < keyNum; i++) {
            Range r = new Range();
            r.readFields(in);
            pathRange[i] = r;
            if (i > 0)
                keyOffsets[i] = keyOffsets[i - 1] + pathRange[i - 1].getKeyLen();
            subkeyLengths[i] = r.getSubkeyLen();
        }
        compositeKeyLen = keyNum > 0 ? keyOffsets[keyNum - 1] + pathRange[keyNum - 1].getKeyLen() : 0;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        HblUtil.writeVarUint32(out, pathRange.length);
        for (int i = 0; i < pathRange.length; i++)
            pathRange[i].write(out);
    }

    @Override
    public boolean filterRowKey(final byte[] buffer, final int rowKeyOffset, final int rowLength) {

        for (int i = 0; i < pathRange.length; i++) {
            int keyLen, keyOffset;
            int comp =
                Bytes.BYTES_RAWCOMPARATOR.compare(buffer, keyOffset = rowKeyOffset + keyOffsets[i], keyLen =
                    pathRange[i].getKeyLen(), pathRange[i].getLeftBound(), 0, keyLen);
            if (comp < 0 || comp == 0 && pathRange[i].isLeftOpen()) {
                if (setHint2LowerBound(i, buffer, rowKeyOffset, rowLength, false))
                    return true;
                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }
            comp =
                Bytes.BYTES_RAWCOMPARATOR.compare(buffer, keyOffset, keyLen, pathRange[i].getRightBound(), 0, keyLen);
            if (comp > 0 || comp == 0 && pathRange[i].isRightOpen()) {
                if (setHint2LowerBound(i, buffer, rowKeyOffset, rowLength, true))
                    return true;
                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }

            // filter out wrong hierarchy depths
            // which is signified by filtering more than 0 of
            // 0x0 bytes on the right side of the key.
            // in other words, if there' that many 0x0 at the end,
            // fitler it out.

            int z = subkeyLengths[i];
            if ( z > 0 ) {
                if ( buffer[keyOffset+z-1]==0)
                    return true;
            }
            if ( z < keyLen) { 
                if ( buffer[keyOffset+z] != 0)
                    return false;
            }
            
        }
        nextKeyCode = ReturnCode.INCLUDE;
        return false;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        return nextKeyCode;
    }

    /**
     * 
     * @param lower
     *            return lower bound if true; otherwise, return upper bound.
     * @return lower or upper bound for the composite index scan given set of
     *         ranges.
     */
    public byte[] getCompositeBound(boolean lower) {
        byte[] bound = new byte[compositeKeyLen];
        if (lower) {
            for (int i = 0; i < pathRange.length; i++)
                System.arraycopy(pathRange[i].getLeftBound(), 0, bound, keyOffsets[i], pathRange[i].getKeyLen());
        } else {
            for (int i = 0; i < pathRange.length; i++)
                System.arraycopy(pathRange[i].getRightBound(), 0, bound, keyOffsets[i], pathRange[i].getKeyLen());
        }
        return bound;
    }

    /**
     * 
     * @param dimIndex
     * @param key
     * @param keyOffset
     * @param keyLength
     * @param plus1
     * @return true if it was the last key, no more ranges
     */
    private boolean setHint2LowerBound(final int dimIndex,
                                       final byte[] compositeKey,
                                       int compositeKeyOffset,
                                       int compositeKeyLength,
                                       boolean plus1) {

        int keyOffset = keyOffsets[dimIndex];
        int keyLen = pathRange[dimIndex].getKeyLen();

        System.arraycopy(compositeKey, compositeKeyOffset, nextKeyHint, 0, keyOffset);
        if (plus1) {
            if (HblUtil.incrementKey(nextKeyHint, 0, keyOffset))
                return true;
        }
        System.arraycopy(pathRange[dimIndex].getLeftBound(), 0, nextKeyHint, keyOffset, keyLen);

        if (dimIndex < pathRange.length - 1)
            Arrays.fill(nextKeyHint, keyOffset = keyOffsets[dimIndex + 1], compositeKeyLen - keyOffset, (byte) 0);
        return false;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(nextKeyHint, 0l);
    }

}
