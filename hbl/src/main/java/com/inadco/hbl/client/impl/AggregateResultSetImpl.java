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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.inadco.datastructs.InputIterator;
import com.inadco.datastructs.adapters.GroupingIterator;
import com.inadco.datastructs.adapters.NWayMergingIterator;
import com.inadco.datastructs.util.StatefulHeapSortMergeStrategy;
import com.inadco.hbl.api.AggregateFunction;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.AggregateResult;
import com.inadco.hbl.client.AggregateResultSet;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.impl.scanner.FilteringScanSpecScanner;
import com.inadco.hbl.client.impl.scanner.GroupingScanStrategy;
import com.inadco.hbl.client.impl.scanner.RawScanResult;
import com.inadco.hbl.client.impl.scanner.ScanSpec;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.util.IOUtil;

/**
 * Aggregated result set implementation .
 * 
 * @author dmitriy
 * 
 */
public class AggregateResultSetImpl implements AggregateResultSet, AggregateResult {

    private Deque<Closeable>                 closeables = new ArrayDeque<Closeable>();
    private static Logger                    s_log      = Logger.getLogger(AggregateResultSetImpl.class);

    private InputIterator<RawScanResult>     delegate;
    private Map<String, Integer>             measureName2IndexMap;
    private AggregateFunctionRegistry        afr;
    private Aggregation[]                    result;
    private Map<String, Integer>             dim2GroupKeyOffsetMap;
    private Map<String, ? extends Dimension> groupDimName2Dimension;
    private Cuboid                           cuboid;

    AggregateResultSetImpl(final List<ScanSpec> scanSpecs,
                           final ExecutorService es,
                           final HTablePool tpool,
                           final AggregateFunctionRegistry afr,
                           final Map<String, Integer> measureName2IndexMap,
                           final Map<String, Integer> dimName2GroupKeyOffsetMap) throws IOException {
        super();

        Validate.notNull(scanSpecs);
        Validate.notEmpty(scanSpecs);
        Validate.notNull(measureName2IndexMap);

        this.measureName2IndexMap = measureName2IndexMap;
        this.dim2GroupKeyOffsetMap = dimName2GroupKeyOffsetMap;
        this.afr = afr == null ? new AggregateFunctionRegistry() : afr;

        ScanSpec spec = scanSpecs.get(0);
        this.cuboid = spec.getCuboid();
        this.groupDimName2Dimension = cuboid.getParentCube().getDimensions();

        // constructors of FilteringScanSpecScanner are the ones that will be
        // running initial query -- so we probably
        // want to parallelize them. except for the first one which we want to
        // run in
        // the context of the current thread.

        List<Future<FilteringScanSpecScanner>> filteringScannerConstructors =
            new ArrayList<Future<FilteringScanSpecScanner>>();

        Iterator<ScanSpec> iter = scanSpecs.iterator();
        ScanSpec firstSpec = iter.next();

        for (; iter.hasNext();) {
            final ScanSpec ss = iter.next();

            Callable<FilteringScanSpecScanner> callable = new Callable<FilteringScanSpecScanner>() {

                @Override
                public FilteringScanSpecScanner call() throws IOException {
                    return new FilteringScanSpecScanner(ss, tpool);
                }
            };

            filteringScannerConstructors.add(es.submit(callable));
        }
        List<FilteringScanSpecScanner> filteringScanners = new ArrayList<FilteringScanSpecScanner>();

        // launch first scanner
        // in the context of this thread

        IOException lastExc = null;
        try {
            FilteringScanSpecScanner fscanner = new FilteringScanSpecScanner(firstSpec, tpool);
            closeables.addFirst(fscanner);
            filteringScanners.add(fscanner);
        } catch (IOException exc) {
            lastExc = exc;
            s_log.error(lastExc);
        }

        // wait for all other parallel dudes to complete.
        for (Future<FilteringScanSpecScanner> fsss : filteringScannerConstructors) {
            try {
                FilteringScanSpecScanner fscanner = fsss.get();
                closeables.addFirst(fscanner);
                filteringScanners.add(fscanner);
            } catch (ExecutionException exc) {
                Throwable thr = exc.getCause();
                if (thr instanceof IOException)
                    lastExc = (IOException) thr;
                else
                    lastExc = new IOException(thr.getMessage(), thr);
                s_log.error(lastExc);
            } catch (InterruptedException exc) {
                lastExc = new IOException("Interrupted", exc);
            }

        }

        @SuppressWarnings("unchecked")
        InputIterator<RawScanResult>[] inputs = new InputIterator[filteringScanners.size()];

        int i = 0;
        for (FilteringScanSpecScanner filteredScanner : filteringScanners) {

            GroupingScanStrategy gsc = new GroupingScanStrategy(filteredScanner.getScanSpec(), afr, false);
            final InputIterator<RawScanResult> groupingScanner =
                new GroupingIterator<RawScanResult, RawScanResult>(filteredScanner, gsc);
            closeables.addFirst(groupingScanner);
            inputs[i] = groupingScanner;
        }
        filteringScanners = null;

        InputIterator<RawScanResult> mergingIter;

        if (inputs.length > 1) {

            /*
             * we have more than one input and have to decorate them with N-way
             * merge in order to proceed.
             */

            StatefulHeapSortMergeStrategy<RawScanResult> sortMergeStrategy =
                new StatefulHeapSortMergeStrategy<RawScanResult>(new RawScanResult.GroupComparator());
            mergingIter = new NWayMergingIterator<RawScanResult>(inputs, sortMergeStrategy, false);
            closeables.addFirst(mergingIter);
        } else {

            // no merging
            mergingIter = inputs[0];
        }

        /*
         * final grouping decoration -- we need to add this if we have merging
         * AND grouping is enabled. The following condition checks just for
         * that.
         */
        if (!(mergingIter instanceof GroupingIterator)) {

            // grouping enabled. Decorate with grouping iterator.
            GroupingScanStrategy gsc = new GroupingScanStrategy(spec, afr, true);
            delegate = new GroupingIterator<RawScanResult, RawScanResult>(mergingIter, gsc);
            closeables.addFirst(delegate);
        } else {
            // no grouping. no decoration.
            delegate = mergingIter;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public void next() throws IOException {
        delegate.next();
        if (result == null)
            result = new Aggregation[delegate.current().getMeasures().length];
        else
            Arrays.fill(result, null);

    }

    @Override
    public AggregateResult current() throws IOException {
        return this;
    }

    @Override
    public int getCurrentIndex() throws IOException {
        return delegate.getCurrentIndex();
    }

    @Override
    public void close() throws IOException {
        IOUtil.closeAll(closeables);
    }

    @Override
    public double getDoubleAggregate(String measure, String functionName) throws HblException {
        try {
            Integer index = measureName2IndexMap.get(measure);
            if (index == null)
                throw new HblException(String.format("Invalid measure name:%s.", measure));
            AggregateFunction af = afr.findFunction(functionName);
            if (af == null)
                throw new HblException(String.format("Invalid function name:%s.", functionName));
            if (result == null)
                throw new HblException("no current result");
            Aggregation measureAggr = result[index];
            if (measureAggr == null) {
                measureAggr = delegate.current().getMeasures()[index].build();
                result[index] = measureAggr; // cache
            }

            return af.getDoubleValue(measureAggr);
        } catch (IOException exc) {
            throw new HblException(exc.getMessage(), exc);
        }

    }

    @Override
    public Object getGroupMember(String dimensionName) throws HblException {
        if (result == null)
            throw new HblException("no current result");
        try {
            Integer offset = dim2GroupKeyOffsetMap.get(dimensionName);
            if (offset == null)
                throw new HblException(String.format("Dimension '%s' is not part of the group.", dimensionName));

            byte[] group = delegate.current().getGroup();
            Dimension dim = groupDimName2Dimension.get(dimensionName);
            Validate.notNull(dim);

            return dim.getMember(group, offset);
        } catch (IOException exc) {
            throw new HblException(exc.getMessage(), exc);
        }

    }

}
