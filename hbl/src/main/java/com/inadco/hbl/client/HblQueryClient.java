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
package com.inadco.hbl.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.springframework.core.io.Resource;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.client.impl.AggregateQueryImpl;
import com.inadco.hbl.client.impl.PreparedAggregateQueryImpl;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.model.SimpleCube;
import com.inadco.hbl.util.IOUtil;

/**
 * HBL query client implementation
 * <P>
 * 
 * Warning: right now this does not implement hbase client shutdown, it assumes
 * caller manages hbase client sharing policies, so this won't close any hbase
 * connections (even if it implicitly creates a new one).
 * 
 * @author dmitriy
 * 
 */
public class HblQueryClient implements Closeable {

    private static final int                   DEFAULT_MAX_THREADS = 50;
    private static final int                   DEFAULT_QUEUE_SIZE  = 3;

    private Configuration                      conf;
    private String                             yamlModelStr;
    private ExecutorService                    es;
    private HTablePool                         tpool;
    private AtomicReference<Map<String, Cube>> cubeCache           = new AtomicReference<Map<String, Cube>>();
    private Deque<Closeable>                   closeables          = new ArrayDeque<Closeable>();

    /*
     * ttl for the model in the client, by default, 10 minutes, then need to
     * update.
     */
    private long                               cubeCacheTTL        = 1000 * 60 * 10;

    public HblQueryClient(Configuration conf) throws IOException, HblException {
        this(conf, (String) null, null);
    }

    public HblQueryClient(Configuration conf, String cubeName) throws IOException, HblException {
        this(conf, cubeName, null);
    }

    public HblQueryClient(Configuration conf, String cubeName, int maxThreads) throws IOException, HblException {
        ThreadPoolExecutor tpe =
            new ThreadPoolExecutor(3, maxThreads, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                DEFAULT_QUEUE_SIZE));
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        closeables.addFirst(new IOUtil.ExecutorServiceCloseable(tpe, 30));
        tpe.prestartAllCoreThreads();

        init(conf, tpe);
        if (cubeName != null)
            loadCube(cubeName);
    }

    public HblQueryClient(Configuration conf, String cubeName, ExecutorService es) throws IOException, HblException {
        init(conf, es);
        if (cubeName != null)
            loadCube(cubeName);
    }

    public HblQueryClient(Configuration conf, ExecutorService es) throws IOException {
        init(conf, es);
    }

    public HblQueryClient(Configuration conf, int maxThreads) throws IOException {
        ThreadPoolExecutor tpe =
            new ThreadPoolExecutor(3, maxThreads, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                DEFAULT_QUEUE_SIZE));
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        tpe.prestartAllCoreThreads();
        closeables.addFirst(new IOUtil.ExecutorServiceCloseable(tpe, 30));
        init(conf, tpe);
    }

    public long getCubeCacheTTL() {
        return cubeCacheTTL;
    }

    public void setCubeCacheTTL(long cubeCacheTTL) {
        this.cubeCacheTTL = cubeCacheTTL;
    }

    @Override
    public void close() throws IOException {
        IOUtil.closeAll(closeables);
    }

    public Cube getCube(String cubeName) throws HblException {
        Map<String, Cube> map, update;
        map = cubeCache.get();
        Cube cube = map == null ? null : map.get(cubeName);

        // TTL check
        if (cube != null && (cube instanceof SimpleCube)) {
            SimpleCube scube = (SimpleCube) cube;
            if (System.currentTimeMillis() - scube.getTimestamp() >= cubeCacheTTL) {
                cube = null;
            }
        }

        if (cube == null) {
            cube = loadCube(cubeName);
            if (cube == null)
                throw new HblException(String.format("Unable to find cube %s.", cubeName));

            do {
                map = cubeCache.get();
                update = map == null ? new HashMap<String, Cube>() : new HashMap<String, Cube>(map);
                update.put(cubeName, cube);

            } while (!cubeCache.compareAndSet(map, update));
        }
        return cube;
    }

    public AggregateQuery createQuery() {
        return new AggregateQueryImpl(this, es, tpool);
    }

    public PreparedAggregateQuery createPreparedQuery() {
        return new PreparedAggregateQueryImpl(this, es, tpool);
    }

    private Cube loadCube(String cubeName) throws HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            try {
                Resource yamlModel = HblAdmin.readModelFromHBase(conf, cubeName, HblAdmin.HBL_DEFAULT_SYSTEM_TABLE);
                InputStream is = yamlModel.getInputStream();

                Validate.notNull(is);
                closeables.addFirst(is);

                yamlModelStr = IOUtil.fromStream(is, "utf-8");
                return YamlModelParser.parseYamlModel(yamlModelStr);

            } finally {
                IOUtil.closeAll(closeables);
            }

        } catch (IOException exc) {
            throw new HblException(String.format("Unable to load cube '%s'.", cubeName), exc);
        }
    }

    private void init(Configuration conf, ExecutorService es) throws IOException {
        Validate.notNull(conf);
        this.conf = conf;

        /*
         * Height queue size not only doesn't help but would actually harm,
         * since if we can't allocate all tasks into threads, we will be screwed
         * since we can't finish the tasks unless we consume all the pipes from
         * them. In fact, this is going to be a big problem until we enable some
         * hierarchical scan advised as batches, not scans.
         */
        if (es == null) {
            ThreadPoolExecutor tpe =
                new ThreadPoolExecutor(3, DEFAULT_MAX_THREADS, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                    DEFAULT_QUEUE_SIZE));
            tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
            closeables.addFirst(new IOUtil.ExecutorServiceCloseable(tpe, 30));
            tpe.prestartAllCoreThreads();
            es = tpe;
        }

        Validate.notNull(es);

        this.es = es;
        tpool = new HTablePool(conf, 400);

    }
}
