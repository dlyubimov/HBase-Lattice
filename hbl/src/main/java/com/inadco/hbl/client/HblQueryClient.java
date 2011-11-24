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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.springframework.core.io.Resource;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.client.impl.AggregateQueryImpl;
import com.inadco.hbl.client.impl.PreparedAggregateQueryImpl;
import com.inadco.hbl.compiler.YamlModelParser;
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

    private static final int          DEFAULT_MAX_THREADS = 50;
    private static final int          DEFAULT_QUEUE_SIZE  = 2;

    // private Configuration conf;
    // private String encodedYamlStr;
    private String                    yamlModelStr;
    private ExecutorService           es;
    private HTablePool                tpool;
    private Cube                      cube;
    private Deque<Closeable>          closeables          = new ArrayDeque<Closeable>();

    public HblQueryClient(Configuration conf, String cubeName) throws IOException {
        this(conf, cubeName, null);
    }

    public HblQueryClient(Configuration conf, String cubeName, int maxThreads) throws IOException {
        ThreadPoolExecutor tpe =
            new ThreadPoolExecutor(3, maxThreads, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                DEFAULT_QUEUE_SIZE));
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        Resource yamlModel = HblAdmin.readModelFromHBase(conf, cubeName, HblAdmin.HBL_DEFAULT_SYSTEM_TABLE);
        init(conf, yamlModel, tpe);
    }

    public HblQueryClient(Configuration conf, String cubeName, ExecutorService es) throws IOException {
        Resource yamlModel = HblAdmin.readModelFromHBase(conf, cubeName, HblAdmin.HBL_DEFAULT_SYSTEM_TABLE);
        init(conf, yamlModel, es);
    }

    public HblQueryClient(Configuration conf, Resource yamlModel) throws IOException {
        this(conf, yamlModel, DEFAULT_MAX_THREADS);
    }

    public HblQueryClient(Configuration conf, Resource yamlModel, ExecutorService es) throws IOException {
        init(conf, yamlModel, es);
    }

    public HblQueryClient(Configuration conf, Resource yamlModel, int maxThreads) throws IOException {
        // Hight queue size not only doesn't help but would actually harm, since
        // if we can't allocate all tasks
        // into threads, we will be screwed since we can't finish the tasks
        // unless we consume all the pipes from them.
        // In fact, this is going to be a big problem until we enable some
        // hierarchical scan advised as batches, not scans.
        ThreadPoolExecutor tpe =
            new ThreadPoolExecutor(3, maxThreads, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                DEFAULT_QUEUE_SIZE));
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        init(conf, yamlModel, tpe);
    }

    @Override
    public void close() throws IOException {
        IOUtil.closeAll(closeables);
    }

    private void init(Configuration conf, Resource yamlModel, ExecutorService es) throws IOException {
        Validate.notNull(conf);
        Validate.notNull(yamlModel);

        // Hight queue size not only doesn't help but would actually harm, since
        // if we can't allocate all tasks
        // into threads, we will be screwed since we can't finish the tasks
        // unless we consume all the pipes from them.
        // In fact, this is going to be a big problem until we enable some
        // hierarchical scan advised as batches, not scans.
        if (es == null) {
            ThreadPoolExecutor tpe =
                new ThreadPoolExecutor(3, DEFAULT_MAX_THREADS, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                    DEFAULT_QUEUE_SIZE));
            tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
            es = tpe;
        }

        Validate.notNull(es);

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            // this.conf = conf;
            this.es = es;

            InputStream is = yamlModel.getInputStream();

            Validate.notNull(is);
            closeables.addFirst(is);

            yamlModelStr = IOUtil.fromStream(is, "utf-8");
            // encodedYamlStr = YamlModelParser.encodeCubeModel(yamlModelStr);
            cube = YamlModelParser.parseYamlModel(yamlModelStr);

            tpool = new HTablePool(conf, 400);

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    public AggregateQuery createQuery() {
        return new AggregateQueryImpl(cube, es, tpool);
    }

    public PreparedAggregateQuery createPreparedQuery() {
        return new PreparedAggregateQueryImpl(cube, es, tpool);
    }
}
