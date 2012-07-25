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
package com.inadco.hbl.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.AggregateFunctionRegistry;
import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.AggregateQuery;
import com.inadco.hbl.client.AggregateResultSet;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.HblQueryClient;
import com.inadco.hbl.client.impl.scanner.ScanSpec;

/**
 * Projection query implementation.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class AggregateQueryImpl implements AggregateQuery {

    protected HblQueryClient            client;
    protected Cube                      cube;
    private ExecutorService             es;
    /**
     * dim name -> range slice requested
     */
    private Map<String, List<Slice>>    dimSlices       = new HashMap<String, List<Slice>>();
    private Set<String>                 measures        = new HashSet<String>();

    protected List<String>              groupDimensions = new ArrayList<String>();
    private HTablePool                  tpool;
    protected AggregateFunctionRegistry afr;
    protected boolean                   allowComplements;

    public AggregateQueryImpl(HblQueryClient client, ExecutorService es, HTablePool tpool) {
        super();
        this.es = es;
        this.tpool = tpool;
        this.client = client;
    }

    public AggregateQuery setCube(String cubeName) throws HblException {
        if (cube == null || !cube.getName().equals(cubeName)) {
            cube = client.getCube(cubeName);
            afr = cube.getAggregateFunctionRegistry();
        }
        return this;
    }

    @Override
    public AggregateQuery addMeasure(String measure) {
        Validate.notNull(measure);
        Validate.notNull(cube, "A cube not set");
        Validate.isTrue(cube.getMeasures().containsKey(measure), "Unknown measure name");
        measures.add(measure);
        return this;
    }

    @Override
    public AggregateQuery addGroupBy(String dimName) {
        Validate.notNull(dimName);
        Validate.notNull(cube, "A cube not set");
        Validate.isTrue(cube.getDimensions().containsKey(dimName), "no such dimension found");

        groupDimensions.add(dimName);
        return this;
    }

    @Override
    public AggregateQuery addClosedSlice(String dimension, Object leftBound, Object rightBound) {
        return addSlice(dimension, leftBound, false, rightBound, false);
    }

    @Override
    public AggregateQuery addOpenSlice(String dimension, Object leftBound, Object rightBound) {
        return addSlice(dimension, leftBound, true, rightBound, true);
    }

    @Override
    public AggregateQuery addHalfOpenSlice(String dimension, Object leftBound, Object rightBound) {
        return addSlice(dimension, leftBound, false, rightBound, true);
    }

    @Override
    public AggregateQuery addSlice(String dimension,
                                   Object leftBound,
                                   boolean leftOpen,
                                   Object rightBound,
                                   boolean rightOpen) {
        Validate.notNull(dimension);
        Validate.notNull(cube, "A cube not set");
        Validate.isTrue(cube.getDimensions().containsKey(dimension));

        if (leftBound == null && rightBound == null) {
            dimSlices.remove(dimension);
            return this;
        }
        List<Slice> sliceSet = dimSlices.get(dimension);
        if (sliceSet == null)
            dimSlices.put(dimension, sliceSet = new ArrayList<Slice>(4));
        sliceSet.add(new Slice(leftBound, leftOpen, rightBound, rightOpen));

        return this;
    }

    public List<ScanSpec> generateScanSpecs(Map<String, Integer> dimName2GroupKeyOffsetMap,
                                            Map<String, Integer> measureName2indexMap) throws IOException, HblException {
        Validate.notNull(cube, "A cube not set");
        Cuboid cuboid = findCuboid();

        Validate.notNull(cuboid, "Unable to find a suitable cuboid for the slice query.");

        /*
         * FIXME, TODO: check slices for overlapping. otherwise, if slices
         * overlap, not only we'd be performing more scans than needed, but they
         * will also contain duplicate counts.
         * 
         * for now we just have to assume that slices will not overlap.
         */
        List<ScanSpec> scanSpecs = new ArrayList<ScanSpec>();

        List<Range> partialSpec = new ArrayList<Range>();

        int groupKeyLen = 0, curKeyLen = 0;

        for (Dimension dim : cuboid.getCuboidDimensions()) {
            String dimName = dim.getName();
            dimName2GroupKeyOffsetMap.put(dimName, curKeyLen);
            curKeyLen += dim.getKeyLen();
            if (groupDimensions.contains(dim.getName()))
                groupKeyLen = curKeyLen;
        }

        // for (int i = 0; i < numGroupKeys; i++) {
        // Dimension dim = cuboid.getCuboidDimensions().get(i);
        // dimName2GroupKeyOffsetMap.put(dim.getName(), groupKeyLen);
        // groupKeyLen += dim.getKeyLen();
        // }

        byte[][] measureQualifiers = new byte[measures.size()][];
        int mCnt = 0;
        for (String mName : measures) {
            measureName2indexMap.put(mName, mCnt);
            measureQualifiers[mCnt++] = Bytes.toBytes(mName);
        }

        Measure[] measuresArr = new Measure[measures.size()];

        int i = 0;
        Map<String, ? extends Measure> measureMap = cube.getMeasures();
        // we already validated measure names are valid during add()
        for (String measure : measures)
            measuresArr[i++] = measureMap.get(measure);

        generateScanSpecs(cuboid, scanSpecs, partialSpec, 0, groupKeyLen, SliceOperation.ADD, measureQualifiers);

        return scanSpecs;

    }

    @Override
    public AggregateResultSet execute() throws HblException {
        try {
            Map<String, Integer> dimName2GroupKeyOffsetMap = new HashMap<String, Integer>();
            Map<String, Integer> measureName2indexMap = new HashMap<String, Integer>();
            List<ScanSpec> scanSpecs = generateScanSpecs(dimName2GroupKeyOffsetMap, measureName2indexMap);

            return createResultSet(scanSpecs,
                                   es,
                                   tpool,
                                   afr,
                                   measureName2indexMap,
                                   dimName2GroupKeyOffsetMap,
                                   null,
                                   null,
                                   null);
        } catch (IOException exc) {
            throw new HblException(exc.getMessage(), exc);
        } finally {
            reset();
        }
    }

    public AggregateResultSet execute(byte[] startSplitKey, byte[] endSplitKey, String enforcedCuboidTableName)
        throws HblException {
        try {
            Map<String, Integer> dimName2GroupKeyOffsetMap = new HashMap<String, Integer>();
            Map<String, Integer> measureName2indexMap = new HashMap<String, Integer>();
            List<ScanSpec> scanSpecs = generateScanSpecs(dimName2GroupKeyOffsetMap, measureName2indexMap);

            return createResultSet(scanSpecs,
                                   es,
                                   tpool,
                                   afr,
                                   measureName2indexMap,
                                   dimName2GroupKeyOffsetMap,
                                   startSplitKey,
                                   endSplitKey,
                                   enforcedCuboidTableName);
        } catch (IOException exc) {
            throw new HblException(exc.getMessage(), exc);
        } finally {
            reset();
        }

    }

    protected void reset() {
        dimSlices.clear();
        measures.clear();
        groupDimensions.clear();
    }

    protected boolean isAllowComplements() {
        return allowComplements;
    }

    protected void setAllowComplements(boolean allowComplements) {
        this.allowComplements = allowComplements;
    }

    /**
     * 
     * @param scanSpecs
     * @param es
     * @param tpool
     * @param afr
     * @param measureName2IndexMap
     * @param dimName2GroupKeyOffsetMap
     * @param startSplitKey
     *            optional: if given, enforce MR split constraints per half-open
     *            [startSplitKey,endSplitKey). note that in this case
     *            endSplitKey can still be null and means
     *            "till the end of the table".
     * @param endSplitKey
     *            optional.
     * @param enforcedCuboidTableName
     *            optional. if passed on, ensures that the cuboid table selected
     *            is the same as given, thus inforcing idempotent cuboid. Used
     *            only by HblInputSplit to assert idempotent optimizer
     *            processing.
     * @return
     * @throws IOException
     */
    protected AggregateResultSetImpl createResultSet(final List<ScanSpec> scanSpecs,
                                                     final ExecutorService es,
                                                     final HTablePool tpool,
                                                     final AggregateFunctionRegistry afr,
                                                     final Map<String, Integer> measureName2IndexMap,
                                                     final Map<String, Integer> dimName2GroupKeyOffsetMap,
                                                     final byte[] startSplitKey,
                                                     final byte[] endSplitKey,
                                                     final String enforcedCuboidTableName) throws IOException {
        return new AggregateResultSetImpl(
            scanSpecs,
            es,
            tpool,
            afr,
            measureName2IndexMap,
            dimName2GroupKeyOffsetMap,
            startSplitKey,
            endSplitKey,
            enforcedCuboidTableName);
    }

    /**
     * Generate cartesian product of all individual dimension scans and also
     * flip scan operation between ADD and COMPLEMENT types
     * 
     * @param cuboid
     * @param scanHolder
     * @param partialSpec
     * @param dimIndex
     * @param numGroupKeys
     * @param so
     */
    private void generateScanSpecs(Cuboid cuboid,
                                   List<ScanSpec> scanHolder,
                                   List<Range> partialSpec,
                                   int dimIndex,
                                   int groupKeyLen,
                                   SliceOperation so,
                                   byte[][] measureQualifiers) {
        List<Dimension> dimensions = cuboid.getCuboidDimensions();
        if (dimIndex == dimensions.size()) {
            // add leaf
            scanHolder.add(new ScanSpec(
                measureQualifiers,
                groupKeyLen,
                partialSpec.toArray(new Range[dimIndex]),
                cuboid,
                so));
            return;
        }
        Dimension dim = dimensions.get(dimIndex);
        List<Slice> slices = dimSlices.get(dim.getName());
        if (slices == null) {

            // generate 'total' slice
            Range allRange = dim.allRange();

            if (partialSpec.size() == dimIndex)
                partialSpec.add(allRange);
            else
                partialSpec.set(dimIndex, allRange);

            generateScanSpecs(cuboid, scanHolder, partialSpec, dimIndex + 1, groupKeyLen, so, measureQualifiers);
        } else {
            if (slices.size() != 1)
                throw new UnsupportedOperationException(
                    "queries to multiple slices of the same dimension are not supported (yet)!");
            Slice slice = slices.iterator().next();
            Range[] ranges = dim.optimizeSliceScan(slice, allowComplements);

            Validate.notEmpty(ranges);

            for (Range r : ranges) {
                SliceOperation nextSo = so;

                /*
                 * clarification:
                 * 
                 * we introduce slice operation (a complement operation in a
                 * plan or additive operation) just for the dimension to be able
                 * to advise us on most optimal combination of such. Note that a
                 * complement slice S as in A\S becomes a union as in A\(S\S2) =
                 * (A\S) U S2 if S2 \subset S. so due to similar argumentation
                 * accross different dimensions, we just invert slice operations
                 * in each subsequent dimension encountered (since each
                 * subsequent dimension always operates on a subset of prior
                 * dimension's hyper slice).
                 */
                if (r.getSliceOperation() == SliceOperation.COMPLEMENT) {
                    if (nextSo == SliceOperation.ADD)
                        nextSo = SliceOperation.COMPLEMENT;
                    else
                        nextSo = SliceOperation.ADD;
                }
                if (partialSpec.size() == dimIndex)
                    partialSpec.add(r);
                else
                    partialSpec.set(dimIndex, r);
                generateScanSpecs(cuboid, scanHolder, partialSpec, dimIndex + 1, groupKeyLen, nextSo, measureQualifiers);
            }

        }
    }

    private Cuboid findCuboid() {

        /*
         * we need to find cuboid with composite keys where grouping dimensions
         * are stacked on the left but where all of the sliced dimensions are
         * also present (in any position).
         */

        Set<String> dimensionSubset = new HashSet<String>();
        dimensionSubset.addAll(dimSlices.keySet());
        dimensionSubset.addAll(groupDimensions);

        Cuboid cuboid = null;

        // find a suitable cuboid per above with fewest extra dimensions.
        for (Cuboid c : cube.getCuboids()) {

            List<String> cPath = c.getCuboidPath();
            // filter out smaller cubes
            if (cPath.size() < dimensionSubset.size())
                continue;

            // filter out those that don't contain all the dimensions we need
            if (!cPath.containsAll(dimensionSubset))
                continue;

            // now check group dimensions that must be stacked on the left.
            int cnt = groupDimensions.size();
            for (String dimName : cPath) {
                if (groupDimensions.contains(dimName)) {
                    cnt--;
                } else {
                    /*
                     * easy but still quite effective optimization is that if
                     * slices are degenerate, their dimension could be pushed
                     * left in the cuboid without breaking inline grouping
                     * prerequisites. This is surprisingly much more often the
                     * case as degenerate slicing is quite common.
                     */
                    List<Slice> slices = dimSlices.get(dimName);
                    if (slices == null || slices.size() != 1)
                        break; // clearly not degenerate

                    Slice slice = slices.get(0);
                    if (slice.isLeftOpen() || slice.isRightOpen()
                        || !slice.getLeftBound().equals(slice.getRightBound()))
                        break; // not degenerate.
                }
            }
            if (cnt > 0)
                continue;

            // found qualifying cuboid. good.
            if (cuboid == null || cPath.size() < cuboid.getCuboidPath().size())
                cuboid = c;
        }

        return cuboid;

    }

}
