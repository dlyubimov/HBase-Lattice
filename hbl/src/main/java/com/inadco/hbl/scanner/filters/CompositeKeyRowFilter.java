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
package com.inadco.hbl.scanner.filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.compiler.YamlModelParser;
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
    private String               modelStr;
    private String               cuboidPath;
    private Range[]              pathRange;

    private transient Cube       model;
    private transient Cuboid     cuboid;
    private transient byte[]     nextKeyHint;
    private transient ReturnCode nextKeyCode;

    public CompositeKeyRowFilter(String modelStr, String cuboidPath, Range[] pathRange) throws IOException {
        super();

        Validate.notNull(modelStr);
        Validate.notNull(cuboidPath);
        Validate.notNull(pathRange);

        this.modelStr = modelStr;
        this.cuboidPath = cuboidPath;
        this.pathRange = pathRange;

        // if nothing for but just validate the model.
        initModel();
    }

    public CompositeKeyRowFilter() {
        super();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO:
        // we of course do not want to ship off the model with
        // each scan request.
        // instead, we probably can initialize from hbase itself
        // once we have proof of concept this stuff is working,
        // we'd just fetch the model needed based on a model key from
        // a system table, perhaps bypassing the YAML serialization by
        // having a writable cache there.
        // -d
        modelStr = in.readUTF();
        cuboidPath = in.readUTF();
        initModel();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(modelStr);
        out.writeUTF(cuboidPath);
        HblUtil.writeVarUint32(out, pathRange.length);

    }

    private void initModel() throws IOException {
        model = YamlModelParser.decodeCubeModel(modelStr);
        cuboid = model.findCuboidForPath(HblUtil.decodeCuboidPath(cuboidPath));
        if (cuboid == null)
            throw new IOException("Unable to find specified cuboid in the model.");

        if (pathRange.length != cuboid.getCuboidDimensions().size())
            throw new IOException("ranges supplied do not correspond to cuboid dimension count");

    }

    @Override
    public boolean filterRowKey(final byte[] buffer, final int keyOffset, final int length) {

        Validate.isTrue(length == cuboid.getKeyLen());

        int len, offset = keyOffset;
        int i = 0;
        for (Dimension dim : cuboid.getCuboidDimensions()) {

            int cLower =
                dim.getMemberComparator().compare(buffer,
                                                  offset,
                                                  len = dim.getKeyLen(),
                                                  pathRange[i].getLeftBound(),
                                                  0,
                                                  len);
            if (cLower < 0 || cLower == 0 && pathRange[i].isLeftOpen()) {

                if (setHint2LowerBound(i, buffer, keyOffset, length, false))
                    return true;
                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }

            int cUpper = dim.getMemberComparator().compare(buffer, offset, len, pathRange[i].getRightBound(), 0, len);
            if (cUpper > 0 || (cUpper == 0 && pathRange[i].isRightOpen())) { // overshoot
                if (setHint2LowerBound(i, buffer, keyOffset, length, true))
                    return true;
                nextKeyCode = ReturnCode.SEEK_NEXT_USING_HINT;
                return false;
            }

            i++;
            offset += len;

        }
        nextKeyCode = ReturnCode.INCLUDE;
        return true;
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
        byte[] result = new byte[cuboid.getKeyLen()];
        int offset = 0, len, i = 0;
        for (Dimension dim : cuboid.getCuboidDimensions()) {
            System.arraycopy(pathRange[i++].getLeftBound(), 0, result, offset, len = dim.getKeyLen());
            offset += len;
        }
        return result;
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
                                       final byte[] key,
                                       final int keyOffset,
                                       final int keyLength,
                                       boolean plus1) {

        List<Dimension> dimensions = cuboid.getCuboidDimensions();
        int offset = keyOffset, len;

        for (int i = 0; i < dimIndex; i++) {
            offset += dimensions.get(i).getKeyLen();
        }
        // copy current keys in the left-side
        if (offset > 0)
            System.arraycopy(key, keyOffset, nextKeyHint, 0, offset - keyOffset);
        if (plus1) {
            if (incrementKey(nextKeyHint, 0, offset - keyOffset))
                return true;
        }
        // copy lower bound for the current key
        System.arraycopy(pathRange[dimIndex].getLeftBound(), 0, nextKeyHint, offset, len =
            dimensions.get(dimIndex).getKeyLen());
        offset += len;
        // set the rest to zeros.
        Arrays.fill(nextKeyHint, offset, cuboid.getKeyLen() - (offset - keyOffset), (byte) 0);
        return false;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(nextKeyHint, 0l);
    }

    /**
     * increment key by 1, catch situations with overflow. Hopefully, since it
     * is a little bit more purposed, it would be a little more efficent than
     * {@link Bytes#incrementBytes(byte[], long)}.
     * <P>
     * 
     * @param key
     * @param offset
     * @param length
     * @return true if increment resulted in overflow (such as FF->00), so no
     *         more keys.
     */
    private boolean incrementKey(byte[] key, int offset, int length) {
        if (length == 0)
            return true;

        int i;
        for (i = offset + length - 1; i >= offset; i--) {
            key[i] = (byte) (1 + key[i]);
            if (key[i] != 0)
                break;
        }
        return !(i >= offset);
    }

}
