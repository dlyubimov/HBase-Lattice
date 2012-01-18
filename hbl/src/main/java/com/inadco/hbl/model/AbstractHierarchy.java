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

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Hierarchy;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.impl.Slice;
import com.inadco.hbl.util.HblUtil;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Abstract Hierarchy support.
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
        r.setSubkeyLen(0);
        return r;
    }

    // @Override
    public Range[] optimizeSliceScanWIP(Slice slice, boolean allowComplements) {
        int keylen = getKeyLen();
        byte[] leftKey, rightKey, rightKeyOpen, leftKeyOpen;
        int depth = getDepth();
        Range[] result = new Range[(depth - 1) * 2 - 1];
        int scans = 0;

        /*
         * level=0 assumes all range. so it is not interesting here.
         */

        leftKey = new byte[keylen];
        rightKey = new byte[keylen];

        Object leftBound = slice.getLeftBound();
        if (leftBound != null)
            getKey(leftBound, depth - 1, leftKey, 0);

        Object rightBound = slice.getRightBound();
        if (rightBound != null)
            getKey(rightBound, depth - 1, rightKey, 0);
        else
            Arrays.fill(rightKey, (byte) 0xff);

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

            int subkeyLen = getSubkeyLen(i);
            byte[] leftInnerClosedKey = getLeftClosedScanKey(leftKeyOpen, subkeyLen);
            byte[] rightInnerClosedKey = getRightClosedScanKey(rightKeyOpen, subkeyLen);

            if (Bytes.compareTo(leftInnerClosedKey, 0, subkeyLen, rightKeyOpen, 0, subkeyLen) == 0) {

                Range r = new Range(leftKey, rightKey, true);
                r.setKeyLen(keylen);
                r.setSubkeyLen(getSubkeyLen(i+1));
                
                // TODO: this is wrong in general case, adjust
                r.setLeftOpen(leftBound == null ? false : slice.isLeftOpen());
                r.setRightOpen(rightBound == null ? false : slice.isRightOpen());
                
                result[scans++] = r;
                break;
            }

            // main scan
            Range r = new Range(leftInnerClosedKey, rightInnerClosedKey, true, false, false);
            r.setKeyLen(subkeyLen);
            result[scans++] = r;

            // test for left gap
            if ( !HblUtil.test1(leftKeyOpen, subkeyLen, keylen-subkeyLen)) { 
                // left gap scan TODO
            }
            if ( ! HblUtil.test0(rightKeyOpen, subkeyLen, keylen-subkeyLen)) { 
                // right gap scan
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
}
