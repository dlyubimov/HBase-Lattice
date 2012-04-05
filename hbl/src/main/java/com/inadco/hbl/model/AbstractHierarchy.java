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
package com.inadco.hbl.model;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Hierarchy;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.impl.Slice;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.util.HblUtil;

/**
 * Abstract Hierarchy support.
 * <P>
 * 
 * We assume that values 0x0,... are reserved for nil level keys. I.e. level key
 * cannot take value 0x0 (unlike regular dimension).
 * <P>
 * 
 * the hierarchy hbase record key thus consists of level1-key level2-key ...
 * levelN-key.
 * <P>
 * 
 * If key corresponds to level k, then keys for levels 1..k are not nil and keys
 * k+1..N are all nils. That's basically how we recognize level on a key in
 * hbase filter.
 * <P>
 * 
 * It follows that level0 hbase record key ([ALL] level) is all 0's and there's
 * no specific level key for [ALL] per se.
 * <P>
 * 
 * 
 * @author dmitriy
 * 
 */
public abstract class AbstractHierarchy extends AbstractDimension implements Hierarchy {

    public AbstractHierarchy(String name, String[] hierarchyPath) {
        super(name);
    }

    @Override
    public void getKey(Object member, byte[] buff, int offset) {
        // by default, just evaluate hierarchy at deepest level.
        getKey(member, getDepth() - 1, buff, offset);
    }

    @Override
    public Range allRange() {
        /*
         * By default, we support [all] range of a dimension by a key with level
         * = 0. (all 0s in all positions, which means dimension cannot really
         * have all-zero keys as a valid value).
         */
        byte[] allKey = new byte[getKeyLen()];
        Range r = new Range(allKey, true);
        // marker for the 'all' key:
        r.setLevelLen(0);
        r.setLevelOffset(0);
        return r;
    }

    @Override
    public Range[] optimizeSliceScan(Slice slice, boolean allowComplements) {
        return optimizeHierarchySliceScan(slice, allowComplements);
    }

    protected Range[] optimizeHierarchySliceScan(Slice slice, boolean allowComplements) {

        /*
         * This actually works bottom-up: take the lowest level, then try to fit
         * the biggest contained level+1 on top of it and add 'left' and 'right'
         * gap scans for those ranges that didn't fit into 'level+1' scan, to
         * 'level' scan; then repeat for level+1 scan until we can't form
         * non-zero scans anymore or reach level=1 (level=0 is [ALL], i.e. all
         * range level key, so we ignore it as atypical for the purposes of
         * slicing. All unspecified dimensions/hierarchies already imply [ALL]
         * range, so if something is specified, it is unlikely [ALL].).
         */

        int keylen = getKeyLen();
        byte[] leftKey, rightKey, rightKeyOpen, leftKeyOpen;
        boolean leftOpen, rightOpen;
        int depth = getDepth();
        Range[] result = new Range[(depth - 1) * 2 - 1];
        int scans = 0;
        int subkeylen = keylen;

        /*
         * level=0 assumes all range. so it is not interesting here.
         */

        leftKey = new byte[keylen];
        rightKey = new byte[keylen];

        Object leftBound = slice.getLeftBound();
        if (leftBound != null) {
            getKey(leftBound, depth - 1, leftKey, 0);
            leftOpen = slice.isLeftOpen();
        } else {
            leftOpen = false;
        }

        Object rightBound = slice.getRightBound();
        if (rightBound != null) {
            getKey(rightBound, depth - 1, rightKey, 0);
            rightOpen = slice.isRightOpen();
        } else {
            Arrays.fill(rightKey, (byte) 0xff);
            rightOpen = false;
        }

        // convert to half-open
        if (!slice.isLeftOpen()) {
            leftKeyOpen = Arrays.copyOf(leftKey, keylen);
            HblUtil.decrementKey(leftKeyOpen, 0, keylen);
        } else {
            leftKeyOpen = leftKey;
        }

        if (!slice.isRightOpen()) {
            rightKeyOpen = Arrays.copyOf(rightKey, keylen);
            HblUtil.incrementKey(rightKeyOpen, 0, keylen);
        } else {
            rightKeyOpen = rightKey;
        }

        for (int i = depth - 2; i > 0; i--) {

            int thisSubkeyLen = getSubkeyLen(i);
            byte[] leftInnerClosedKey = getLeftClosedScanKey(leftKeyOpen, thisSubkeyLen);
            byte[] rightInnerClosedKey = getRightClosedScanKey(rightKeyOpen, thisSubkeyLen);

            if (Bytes.compareTo(leftInnerClosedKey, 0, thisSubkeyLen, rightInnerClosedKey, 0, thisSubkeyLen) > 0) {

                Range r = new Range(leftKey, rightKey, true);
                r.setKeyLen(keylen);
                r.setLevelOffset(thisSubkeyLen);
                r.setLevelLen(getSubkeyLen(i + 1) - thisSubkeyLen);

                // TODO: this is wrong in general case, adjust
                r.setLeftOpen(leftBound == null ? false : slice.isLeftOpen());
                r.setRightOpen(rightBound == null ? false : slice.isRightOpen());

                result[scans++] = r;
                break;
            }

            // test for left gap
            if (!HblUtil.test1s(leftKeyOpen, thisSubkeyLen, keylen - thisSubkeyLen)) {
                // left gap scan
                Range leftGapR = new Range(leftKey, leftInnerClosedKey, true, leftOpen, true);
                leftGapR.setLevelOffset(thisSubkeyLen);
                leftGapR.setLevelLen(subkeylen - thisSubkeyLen);
                leftGapR.setSliceOperation(SliceOperation.ADD);
                result[scans++] = leftGapR;

            }
            // test for right gap
            if (!HblUtil.test0s(rightKeyOpen, thisSubkeyLen, keylen - thisSubkeyLen)) {
                // right gap scan
                byte[] rightStart = rightInnerClosedKey.clone();
                HblUtil.incrementKey(rightStart, 0, thisSubkeyLen);
                Range rightGapR = new Range(rightStart, rightKey, true, true, rightOpen);
                rightGapR.setKeyLen(keylen);
                rightGapR.setLevelOffset(thisSubkeyLen);
                rightGapR.setLevelLen(subkeylen - thisSubkeyLen);
                rightGapR.setSliceOperation(SliceOperation.ADD);
                result[scans++] = rightGapR;
            }

            // more upkeys?

            if (i == 1) {
                // main scan
                Range r = new Range(leftInnerClosedKey, rightInnerClosedKey, true, false, false);
                r.setKeyLen(keylen);
                r.setLevelOffset(0);
                r.setLevelLen(thisSubkeyLen);

                r.setSliceOperation(SliceOperation.ADD);
                result[scans++] = r;
                break;

            } else {
                leftKey = leftInnerClosedKey;
                rightKey = rightInnerClosedKey;
                leftOpen = false;
                rightOpen = false;
                subkeylen = thisSubkeyLen;

                rightKeyOpen = getRightOpenScanKey(rightKey, thisSubkeyLen);
                leftKeyOpen = getLeftOpenScanKey(leftKey, thisSubkeyLen);
            }

        }

        if (scans < result.length)
            result = (Range[]) Arrays.copyOf(result, scans);

        return result;
    }

    private byte[] getLeftClosedScanKey(byte[] leftOpenKey, int subkeyLen) {
        byte[] l = new byte[leftOpenKey.length];
        System.arraycopy(leftOpenKey, 0, l, 0, subkeyLen);
        HblUtil.incrementKey(l, 0, subkeyLen);
        return l;
    }

    private byte[] getRightClosedScanKey(byte[] rightOpenKey, int subkeyLen) {
        byte[] r = new byte[rightOpenKey.length];
        System.arraycopy(rightOpenKey, 0, r, 0, subkeyLen);
        HblUtil.decrementKey(r, 0, subkeyLen);
        return r;
    }

    private byte[] getLeftOpenScanKey(byte[] leftClosedKey, int subkeyLen) {
        byte[] l = new byte[leftClosedKey.length];
        System.arraycopy(leftClosedKey, 0, l, 0, subkeyLen);
        HblUtil.decrementKey(l, 0, subkeyLen);
        return l;
    }

    private byte[] getRightOpenScanKey(byte[] rightClosedKey, int subkeyLen) {
        byte[] r = new byte[rightClosedKey.length];
        System.arraycopy(rightClosedKey, 0, r, 0, subkeyLen);
        HblUtil.incrementKey(r, 0, subkeyLen);
        return r;
    }
}
