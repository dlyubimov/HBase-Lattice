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
import java.util.Arrays;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Range;
import com.inadco.hbl.util.HblUtil;


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

    /*
     * of course since we are using Writable serialization rather than java
     * native, keyword 'transient' doesn't mean anything in this context, but
     * i'd like to use it as a marker for something i don't really serialize.
     */
    private transient int[]      keyOffsets;
    private transient int        compositeKeyLen;
    private transient byte[]     nextKeyHint;
    private transient ReturnCode nextKeyCode;
    private transient int        rowsSeen;

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
        for (int i = 0; i < keyNum; i++) {
            Range r = new Range();
            r.readFields(in);
            pathRange[i] = r;
        }
        initTransients();
    }

    public void initTransients() {
        int keyNum = pathRange.length;
        keyOffsets = new int[keyNum];
        for (int i = 0; i < keyNum; i++) {
            if (i > 0)
                keyOffsets[i] = keyOffsets[i - 1] + pathRange[i - 1].getKeyLen();
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
        rowsSeen++;

        for (int i = 0; i < pathRange.length; i++) {
            int keyLen, keyOffset;
            Range r = pathRange[i];

            int comp =
                Bytes.BYTES_RAWCOMPARATOR.compare(buffer,
                                                  keyOffset = rowKeyOffset + keyOffsets[i],
                                                  keyLen = r.getKeyLen(),
                                                  r.getLeftBound(),
                                                  0,
                                                  keyLen);
            if (comp < 0 || comp == 0 && pathRange[i].isLeftOpen()) {
                /*
                 * if we are at rightmost key, doesn't make sense to reseek,
                 * just skip, as we know next hint would be next row.
                 */
                if (comp == 0 && i == pathRange.length - 1)
                    return true;

                if (setHint2LowerBound(i, buffer, rowKeyOffset, rowLength, false))
                    return true;

                /*
                 * if we are exactly at the left-open border already: increment
                 */

                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }
            comp = Bytes.BYTES_RAWCOMPARATOR.compare(buffer, keyOffset, keyLen, r.getRightBound(), 0, keyLen);
            if (comp > 0 || comp == 0 && r.isRightOpen()) {
                if (setHint2LowerBound(i, buffer, rowKeyOffset, rowLength, true))
                    return true;
                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }

            /*
             * level is greater than ours, we need to skip to the next slice at
             * our level.
             */
            if (testLevelGreater(i, buffer, rowKeyOffset)) {
                setHint2NextHierarchicalLevelKey(i, buffer, rowKeyOffset, rowLength);
                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }

            /*
             * if current key has level less than current level -- just skip it.
             * This technically is never supposed to happen because less level
             * keys would be filtered out by low boundary comparison (they
             * always less than lower boundary).
             */
            if (testLevelLess(i, buffer, rowKeyOffset)) {
                return true;
            }

        }
        nextKeyCode = ReturnCode.INCLUDE;
        return false;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        if (nextKeyCode == ReturnCode.SEEK_NEXT_USING_HINT) {

            /*
             * just a safeguard against indefinite loop. Once we iron out all
             * problems with reseeks, we can remove this.
             */

            if (Bytes.compareTo(kv.getRow(), nextKeyHint) < 0)
                return nextKeyCode;
            else
                return ReturnCode.INCLUDE;
        }
        return nextKeyCode;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(nextKeyHint, 0l);
    }

    /**
     * 
     * @param lower
     *            return lower bound if true; otherwise, return upper bound.
     * @return lower or upper bound for the composite index scan given set of
     *         ranges.
     */
    public byte[] getCompositeBound(boolean lower) {
        if (keyOffsets == null)
            initTransients();
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

    public int getRowsSeen() {
        return rowsSeen;
    }

    /**
     * Adjust hint to the next hierarchy member key (assuming level < maxdepth).
     * 
     * @param dimIndex
     *            current dimension range index
     * @param compositeKey
     *            composite key buffer
     * @param compositeKeyOffset
     *            offset of composite key
     * @param composteKeyLength
     *            length of entire composite key
     * @param keylen
     *            keylength of hierarchical key corresponding to dimension
     *            dimIndex
     * @param subkeyLength
     *            level key length of hierarchy dimIndex
     */
    private void setHint2NextHierarchicalLevelKey(int dimIndex,
                                                  final byte[] compositeKey,
                                                  int compositeKeyOffset,
                                                  int compositeKeyLength) {

        int keyOffset = keyOffsets[dimIndex];

        if (nextKeyHint == null)
            nextKeyHint = new byte[compositeKeyLen];

        Range r = pathRange[dimIndex];
        int subkeylen = r.getSubkeyLen();
        int keylen = r.getKeyLen();

        System.arraycopy(compositeKey, compositeKeyOffset, nextKeyHint, 0, keyOffset + subkeylen);
        HblUtil.incrementKey(nextKeyHint, keyOffset, subkeylen);
        Arrays.fill(nextKeyHint, keyOffset + subkeylen, keyOffset + keylen, (byte) 0);
        adjustHint2LowerBound(dimIndex + 1);
    }

    /**
     * 
     * @param dimIndex
     * @param key
     * @param keyOffset
     * @param keyLength
     * @param prefixPlus1
     *            add +1 to prefix part before forming low bound tail.
     * @return true if it was the last key, no more ranges
     */
    private boolean setHint2LowerBound(final int dimIndex,
                                       final byte[] compositeKey,
                                       int compositeKeyOffset,
                                       int compositeKeyLength,
                                       boolean prefixPlus1) {

        int keyOffset = keyOffsets[dimIndex];
        int keyLen = pathRange[dimIndex].getKeyLen();
        int subkeyLen = pathRange[dimIndex].getSubkeyLen();

        if (nextKeyHint == null)
            nextKeyHint = new byte[compositeKeyLen];

        /* copy prefix */
        System.arraycopy(compositeKey, compositeKeyOffset, nextKeyHint, 0, keyOffset);

        if (prefixPlus1) {
            if (HblUtil.incrementKey(nextKeyHint, 0, keyOffset))
                return true;
        }

        /* copy key */
        System.arraycopy(pathRange[dimIndex].getLeftBound(), 0, nextKeyHint, keyOffset, keyLen);

        /* reseek must start at +1 of subkeylen in case of open bound */
        if (pathRange[dimIndex].isLeftOpen())
            HblUtil.incrementKey(nextKeyHint, keyOffset, subkeyLen);

        /* set the "tail" to lower bound */
        adjustHint2LowerBound(dimIndex + 1);
        return false;
    }

    /**
     * test whether current key's hierarchy level is greater than spec'd in
     * range path
     */
    private boolean testLevelGreater(int dimIndex, byte[] compositeKey, int compositeKeyOffset) {
        Range r = pathRange[dimIndex];
        int keylen = r.getKeyLen();
        int nextLevelOffset = r.getLevelOffset() + r.getLevelLen();
        if (nextLevelOffset >= keylen)
            return false;

        int keyOffset = keyOffsets[dimIndex] + compositeKeyOffset;

        return !HblUtil.test0s(compositeKey, keyOffset + nextLevelOffset, keylen - nextLevelOffset);
    }

    /**
     * tests whether current key's hierarchy level is smaller that spec'd in
     * range path
     * 
     * @param dimIndex
     * @param compositeKey
     * @param compositeKeyOffset
     * @return
     */
    private boolean testLevelLess(int dimIndex, byte[] compositeKey, int compositeKeyOffset) {
        Range r = pathRange[dimIndex];
        if (r.getLevelLen() == 0)
            return false; // smallest level already

        int keyOffset = keyOffsets[dimIndex] + compositeKeyOffset;

        return HblUtil.test0s(compositeKey, keyOffset + r.getLevelOffset(), r.getLevelLen());
    }

    private void adjustHint2LowerBound(final int fromDimIndex) {
        for (int dimIndex = fromDimIndex; dimIndex < pathRange.length; dimIndex++) {
            // copy the "lower bound tail"
            int offset = keyOffsets[dimIndex];
            System.arraycopy(pathRange[dimIndex].getLeftBound(),
                             0,
                             nextKeyHint,
                             offset,
                             pathRange[dimIndex].getKeyLen());
            // increment if open left is meant:
            if (pathRange[dimIndex].isLeftOpen()) {
                int subkeylen = pathRange[dimIndex].getSubkeyLen();
                HblUtil.incrementKey(nextKeyHint, offset, subkeylen);
            }

        }
    }

}
