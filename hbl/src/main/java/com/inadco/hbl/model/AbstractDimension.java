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

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.impl.Slice;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Abstract dimension -- common defautl stuff for all dimensions.
 * 
 * @author dmitriy
 * 
 */
public abstract class AbstractDimension implements Dimension {

    protected String name;

    public AbstractDimension(String name) {
        super();
        Validate.notNull(name);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public RawComparator<?> getMemberComparator() {
        return Bytes.BYTES_RAWCOMPARATOR;
    }

    @Override
    public Range[] optimizeSliceScan(Slice slice, boolean allowComplements) {
        int keylen = getKeyLen();
        byte[] leftKey = new byte[keylen], rightKey = new byte[keylen];

        Object leftBound = slice.getLeftBound();
        if (leftBound != null)
            getKey(leftBound, leftKey, 0);

        Object rightBound = slice.getRightBound();
        if (rightBound != null)
            getKey(rightBound, rightKey, 0);
        else
            Arrays.fill(rightKey, (byte) 0xff);
        Range r = new Range(leftKey, rightKey, true);
        r.setKeyLen(keylen);
        r.setSubkeyLen(keylen);
        r.setLeftOpen(leftBound == null ? false : slice.isLeftOpen());
        r.setRightOpen(rightBound == null ? false : slice.isRightOpen());
        return new Range[] { r };

    }

    @Override
    public Range allRange() {
        int keylen = getKeyLen();
        byte[] leftKey = new byte[keylen], rightKey = new byte[keylen];
        Arrays.fill(rightKey, (byte) 0xff);
        Range r = new Range(leftKey, rightKey, true, false, false);
        r.setSubkeyLen(keylen);
        return r;
    }

}
