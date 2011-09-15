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
package com.inadco.hbl.scanner;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.scanner.filters.CompositeKeyRowFilter;
import com.inadco.hbl.util.HblUtil;
import com.inadco.hbl.util.IOUtil;

/**
 * 
 * @author dmitriy
 * 
 */
class SliceScan {

    private static final int          CACHING = 1000;   // TODO: make it
                                                         // parameterizable.

    private HTablePool                pool;
    private String                    cubeModelString;
    private Cube                      cube;
    private Cuboid                    cuboid;
    private Range[]                   slicing;
    private Measure[]                 measures;
    private byte[][]                  measureQualifiers;
    private AggregateFunctionRegistry afr;
    private KeyOperationStrategy      keyStrategy;

    public SliceScan(HTablePool pool,
                     String encodedModelStr,
                     Cube cube,
                     Cuboid cuboid,
                     Range[] slicing,
                     Measure[] measures,
                     KeyOperationStrategy keyStrategy,
                     AggregateFunctionRegistry afr) {
        super();

        Validate.notNull(encodedModelStr);
        Validate.notNull(cube);
        Validate.notNull(pool);
        Validate.notNull(cuboid);
        Validate.notNull(slicing);
        Validate.notNull(measures);
        this.cube = cube;
        this.cuboid = cuboid;
        this.slicing = slicing;
        this.measures = measures;
        this.afr = afr == null ? new AggregateFunctionRegistry() : afr;
        this.keyStrategy = keyStrategy == null ? new DefaultKeyOperationStrategy() : keyStrategy;
        measureQualifiers = new byte[measures.length][];
        int i = 0;
        for (Measure m : measures)
            measureQualifiers[i++] = Bytes.toBytes(m.getName());
    }

    /**
     * return all scan aggregations for all measures
     * 
     * @return
     * @throws IOException
     */
    public void performScan(Aggregation.Builder[] holders) throws IOException {
        Validate.isTrue(holders.length >= measures.length, "number of holders and requested measures does not match");
        // keep this potentially re-entrant, shall we?
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            // Aggregation.Builder[] aggregations = new
            // Aggregation.Builder[measures.length];
            // for ( int i = 0; i < aggregations.length; i++ )
            // aggregations[i]=Aggregation.newBuilder();

            CompositeKeyRowFilter krf =
                new CompositeKeyRowFilter(cubeModelString, HblUtil.encodeCuboidPath(cuboid), slicing);

            HTableInterface table = pool.getTable(cuboid.getCuboidTableName());

            closeables.addFirst(new IOUtil.PoolableHtableCloseable(pool, table));

            byte[] startKey = new byte[cuboid.getKeyLen()], endKey = new byte[cuboid.getKeyLen()];

            int offset = 0;
            List<Dimension> dims = cuboid.getCuboidDimensions();

            for (int i = 0; i < dims.size(); i++) {
                int keylen = dims.get(i).getKeyLen();
                System.arraycopy(slicing[i].getLeftBound(), 0, startKey, offset, keylen);
                System.arraycopy(slicing[i].getRightBound(), 0, endKey, offset, keylen);
                offset += keylen;
            }

            boolean noLowBound = HblUtil.incrementKey(endKey, 0, endKey.length);

            Scan scan = new Scan();
            scan.setStartRow(startKey);
            if (!noLowBound)
                scan.setStopRow(endKey);

            // TODO: we probably need a column filter along with krf to filter
            // out measures not requested to save bandwidth etc. Right now
            // all measure info will be returned (probably along with all
            // of their versions if more than one!)
            scan.setFilter(krf);

            scan.setCaching(CACHING); // TODO: this probably needs to be
                                      // parameterizable

            ResultScanner rscan = table.getScanner(scan);
            closeables.addFirst(rscan);
            Result[] rs;
            int measureLen = measures.length;
            while (null != (rs = rscan.next(CACHING))) {
                for (Result r : rs) {
                    SliceOperation sop = keyStrategy.getKeyOperation(r.getRow(), cuboid);
                    switch (sop) {
                    case IGNORE:
                        break;
                    default:
                        // this should be well optimized, no new objects created
                        // here please.
                        for (int i = 0; i < measureLen; i++) {
                            byte[] v = r.getValue(HblAdmin.HBL_METRIC_FAMILY, measureQualifiers[i]);
                            if (v == null)
                                continue;
                            afr.mergeAll(holders[i], Aggregation.parseFrom(v), sop);
                        }
                    }
                }
            }

        } finally {
            IOUtil.closeAll(closeables);
        }

    }
}
