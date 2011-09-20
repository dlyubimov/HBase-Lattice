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
import java.util.Deque;
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
import com.inadco.datastructs.adapters.NWayMergingIterator;
import com.inadco.datastructs.util.StatefulHeapSortMergeStrategy;
import com.inadco.hbl.api.AggregateFunction;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.AggregateResult;
import com.inadco.hbl.client.AggregateResultSet;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.impl.scanner.FilteringScanSpecScanner;
import com.inadco.hbl.client.impl.scanner.GroupingScanSpecScanner;
import com.inadco.hbl.client.impl.scanner.RawScanResult;
import com.inadco.hbl.client.impl.scanner.ScanSpec;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.util.IOUtil;

import edu.emory.mathcs.backport.java.util.Arrays;

public class AggregateResultSetImpl implements AggregateResultSet, AggregateResult {

    private Deque<Closeable>             closeables = new ArrayDeque<Closeable>();
    private static Logger                s_log      = Logger.getLogger(AggregateResultSetImpl.class);

    private InputIterator<RawScanResult> delegate;
    private Map<String, Integer>         measureName2IndexMap;
    private AggregateFunctionRegistry    afr;
    private Aggregation[]                result;

    AggregateResultSetImpl(final List<ScanSpec> scanSpecs,
                           final ExecutorService es,
                           final HTablePool tpool,
                           final AggregateFunctionRegistry afr,
                           final Map<String, Integer> measureName2IndexMap) throws IOException {
        super();

        Validate.notNull(scanSpecs);
        Validate.notEmpty(scanSpecs);
        Validate.notNull(measureName2IndexMap);
        this.measureName2IndexMap = measureName2IndexMap;
        this.afr = afr == null ? new AggregateFunctionRegistry() : afr;
        // constructors of FilteringScanSpecScanner are the ones that will be
        // running initial query -- so we probably
        // want to parallelize them.

        List<Future<FilteringScanSpecScanner>> filteringScannerConstructors =
            new ArrayList<Future<FilteringScanSpecScanner>>();

        for (final ScanSpec ss : scanSpecs) {

            filteringScannerConstructors.add(es.submit(new Callable<FilteringScanSpecScanner>() {

                @Override
                public FilteringScanSpecScanner call() throws IOException {
                    return new FilteringScanSpecScanner(ss, tpool);
                }
            }));
        }
        List<FilteringScanSpecScanner> filteringScanners = new ArrayList<FilteringScanSpecScanner>();

        IOException lastExc = null;

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

        GroupingScanSpecScanner[] inputs = new GroupingScanSpecScanner[filteringScanners.size()];
        int i = 0;
        for (FilteringScanSpecScanner filteredScanner : filteringScanners) {

            final GroupingScanSpecScanner groupingScanner =
                new GroupingScanSpecScanner(filteredScanner.getScanSpec(), filteredScanner, afr, false);
            closeables.addFirst(groupingScanner);
            inputs[i] = groupingScanner;
        }
        filteringScanners = null;

        StatefulHeapSortMergeStrategy<RawScanResult> sortMergeStrategy =
            new StatefulHeapSortMergeStrategy<RawScanResult>(new RawScanResult.GroupComparator());
        NWayMergingIterator<RawScanResult> mergingIter =
            new NWayMergingIterator<RawScanResult>(inputs, sortMergeStrategy, false);
        closeables.addFirst(mergingIter);

        delegate = new GroupingScanSpecScanner(scanSpecs.get(0), mergingIter, afr, true);
        closeables.addFirst(delegate);

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
            Aggregation measureAggr=result[index];
            if ( measureAggr==null ) { 
                measureAggr= delegate.current().getMeasures()[index].build();
                result[index]=measureAggr; // cache
            }
    
            return af.getDoubleValue(measureAggr);
        } catch (IOException exc ) { 
            throw new HblException ( exc.getMessage(),exc);
        }

    }

    @Override
    public Object getGroupMember(String dimensionName) {
        // TODO Auto-generated method stub
        return null;
    }

}
