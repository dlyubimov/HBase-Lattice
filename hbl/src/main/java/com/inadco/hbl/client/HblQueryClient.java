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
import org.springframework.core.io.Resource;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.client.impl.AggregateQueryImpl;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.util.IOUtil;

public class HblQueryClient {

    private static final int DEFAULT_MAX_THREADS = 50;

    private Configuration    conf;
    private String           encodedYamlStr;
    private String           yamlModelStr;
    private ExecutorService  es;

    private Cube             cube;

    public HblQueryClient(Configuration conf, Resource yamlModel) throws IOException {
        this(conf, yamlModel, DEFAULT_MAX_THREADS);
    }

    public HblQueryClient(Configuration conf, Resource yamlModel, ExecutorService es) throws IOException {
        init(conf, yamlModel, es);
    }

    public HblQueryClient(Configuration conf, Resource yamlModel, int maxThreads) throws IOException {
        ThreadPoolExecutor tpe =
            new ThreadPoolExecutor(3, maxThreads, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(5));
        tpe.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        init(conf, yamlModel, tpe);
    }

    private void init(Configuration conf, Resource yamlModel, ExecutorService es) throws IOException {
        Validate.notNull(conf);
        Validate.notNull(yamlModel);
        Validate.notNull(es);

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            this.conf = conf;
            this.es = es;

            InputStream is = yamlModel.getInputStream();

            Validate.notNull(is);
            closeables.addFirst(is);

            yamlModelStr = IOUtil.fromStream(is, "utf-8");
            encodedYamlStr = YamlModelParser.encodeCubeModel(yamlModelStr);
            cube = YamlModelParser.parseYamlModel(yamlModelStr);

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    AggregateQuery createQuery () { 
        return new AggregateQueryImpl(cube,es);
    }
}
