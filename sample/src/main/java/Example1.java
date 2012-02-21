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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.protobuf.ByteString;
import com.inadco.hb.example1.codegen.Example1.CompilerInput;
import com.inadco.hbl.client.AggregateQuery;
import com.inadco.hbl.client.AggregateResult;
import com.inadco.hbl.client.AggregateResultSet;
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.HblQueryClient;
import com.inadco.hbl.client.PreparedAggregateQuery;
import com.inadco.hbl.client.PreparedAggregateResult;
import com.inadco.hbl.compiler.Pig8CubeIncrementalCompilerBean;
import com.inadco.hbl.math.aggregators.OnlineCannyAvgSummarizer;
import com.inadco.hbl.util.HblUtil;
import com.inadco.hbl.util.IOUtil;

/**
 * to run, use hadoop command line
 * 
 * @author dmitriy
 * 
 */

public class Example1 extends Configured implements Tool {

    public static void main(String[] args) throws Throwable {

        ToolRunner.run(new Example1(), args);

    }

    private static ExecType      EXEC_TYPE  = ExecType.MAPREDUCE;
    private static final boolean QUERY_ONLY = false;

    private HblQueryClient       queryClient;
    private Deque<Closeable>     closeables = new ArrayDeque<Closeable>();

    @Override
    public int run(String[] args) throws Exception {
        try {

            // script resource
            Resource cubeModelRsrc = new ClassPathResource("example1.yaml");

            /*
             * deploy cube schema (optionally dropping the existing one)
             * WARNING: would drop existing cube!!
             */
            HblAdmin hblAdmin = new HblAdmin(cubeModelRsrc);
            if (!QUERY_ONLY) {
                hblAdmin.dropCube(getConf());
                hblAdmin.deployCube(getConf());
            }

            String cubeName = hblAdmin.getCube().getName();

            /*
             * prepare incremental simulated input and select work dir for the
             * compiler job
             */

            FileSystem dfs =
                EXEC_TYPE == ExecType.MAPREDUCE ? FileSystem.get(getConf()) : FileSystem.getLocal(getConf());
            Path workPath = new Path(dfs.getWorkingDirectory(), "hbltemp-" + System.currentTimeMillis());
            Path inputPath = new Path(dfs.getWorkingDirectory(), "sample1-input" + System.currentTimeMillis());

            simulateInput(dfs, inputPath);

            // run compiler for the model
            Pig8CubeIncrementalCompilerBean compiler =
                new Pig8CubeIncrementalCompilerBean(
                    getConf(),
                    cubeName,
                    new ClassPathResource("example1-preambula.pig"),
                    5);
            /*
             * test fact compile time exclusion to allow merging different fact
             * stream sources
             */

            compiler.setMeasureExclude(new HashSet<String>(Arrays.asList("excludedMeasure")));

            // or:
            // compiler.setMeasureInclude(new
            // HashSet<String>(Arrays.asList("impCnt", "click")));

            /*
             * this is the version that uses model from resource instead of hbl
             * system table.
             */
            // new Pig8CubeIncrementalCompilerBean(cubeModelRsrc, new
            // ClassPathResource("example1-preambula.pig"), 5);

            String script = compiler.preparePigSource(workPath.toString());

            // ////////////////////////////////////
            // ------------- debug: dump the script
            Path dumpDir = new Path(inputPath, "__debug");
            dfs.mkdirs(dumpDir);
            Path scriptDumpPath = new Path(dumpDir, "compiler.pig");
            System.out.printf("script saved at:%s\n", scriptDumpPath.toString());
            FSDataOutputStream fsdos = dfs.create(scriptDumpPath);
            try {
                fsdos.writeUTF(script);
            } finally {
                fsdos.close();
            }
            // ------------- debug: dump the script
            // ////////////////////////////////////

            if (!QUERY_ONLY)
                runScript(script, inputPath);

            queryClient = new HblQueryClient(getConf());
            closeables.addFirst(queryClient);

            testClient1(cubeName);
            testClient2(cubeName);
            testClient3(cubeName);
            testClient4(cubeName);

            // query based tests
            testClient5(cubeName);
            testClient6(cubeName);
            testClient7(cubeName);
            testClient8(cubeName);

            return 0;

        } finally {
            IOUtil.closeAllQuietly(closeables);
        }
    }

    private void testClient1(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /*
             * this should be equivalent to select aggr_func(impCnt),
             * aggr_func(click) from ... where dim1<=ids[0] and dim1>=ids[0]
             * group by dim1
             */

            AggregateQuery query = queryClient.createQuery();
            query.setCube(cubeName).addMeasure("impCnt").addMeasure("click");
            query.addClosedSlice("dim1", ids[0], ids[0]).addGroupBy("dim1");
            AggregateResultSet rs = query.execute();
            closeables.addFirst(rs);
            while (rs.hasNext()) {
                rs.next();
                AggregateResult ar = rs.current();
                System.out.printf("%032X sum/cnt: impCnt %.4f/%d, click %.4f/%d\n",
                                  new BigInteger(1, (byte[]) ar.getGroupMember("dim1")),
                                  ar.getAggregate("impCnt", "SUM"),
                                  ar.getAggregate("impCnt", "COUNT"),
                                  ar.getAggregate("click", "SUM"),
                                  ar.getAggregate("click", "COUNT"));
            }

            closeables.remove(rs);
            rs.close();

        } finally {
            IOUtil.closeAll(closeables);
        }

    }

    private void testClient2(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /**
             * Now, more difficult. try to hit both keys lifetime. This will
             * result in composite key filtering with a restart (most
             * fundamental composite key filtering capability but only one part
             * of the key). This now passes too.
             * 
             */
            AggregateQuery query = queryClient.createQuery();

            query.setCube(cubeName).addMeasure("impCnt").addMeasure("click");
            query.addClosedSlice("dim1", ids[0], ids[1]).addGroupBy("dim1");
            AggregateResultSet rs = query.execute();
            closeables.addFirst(rs);
            while (rs.hasNext()) {
                rs.next();
                AggregateResult ar = rs.current();
                System.out.printf("%032X sum/cnt: impCnt %.4f/%d, click %.4f/%d\n",
                                  new BigInteger(1, (byte[]) ar.getGroupMember("dim1")),
                                  ar.getAggregate("impCnt", "SUM"),
                                  ar.getAggregate("impCnt", "COUNT"),
                                  ar.getAggregate("click", "SUM"),
                                  ar.getAggregate("click", "COUNT"));
            }
            closeables.remove(rs);
            rs.close();

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient3(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            HblQueryClient queryClient = new HblQueryClient(getConf());
            closeables.addFirst(queryClient);

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /**
             * same as client2 but print the summaries separately (no grouping).
             * This is obviously not terribly useful, the queries have got to
             * have group specification -- -- unless we group up all of it.
             */
            AggregateQuery query = queryClient.createQuery();

            query.setCube(cubeName).addMeasure("impCnt").addMeasure("click");
            query.addClosedSlice("dim1", ids[0], ids[1])/* .addGroupBy("dim1") */;
            AggregateResultSet rs = query.execute();
            closeables.addFirst(rs);
            while (rs.hasNext()) {
                rs.next();
                AggregateResult ar = rs.current();
                System.out.printf("%s sum/cnt: impCnt %.4f/%d, click %.4f/%d\n",
                // new BigInteger(1,(byte[])ar.getGroupMember("dim1")),
                                  "no-group",
                                  ar.getAggregate("impCnt", "SUM"),
                                  ar.getAggregate("impCnt", "COUNT"),
                                  ar.getAggregate("click", "SUM"),
                                  ar.getAggregate("click", "COUNT"));
            }
            closeables.remove(rs);
            rs.close();

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient4(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            AggregateQuery query = queryClient.createQuery();
            query.setCube(cubeName);
            for (int i = 0; i < 5; i++) {

                query.reset();
                byte ids[][] = new byte[2][];
                ids[0] = new byte[16];
                ids[1] = new byte[16];
                HblUtil.incrementKey(ids[1], 0, 16);

                /*
                 * will try to also constrain for half-open [1:00am,3:00am)
                 */

                GregorianCalendar startTime = IOUtil.tryClone(START_BASE);
                GregorianCalendar endTime = IOUtil.tryClone(START_BASE);

                /*
                 * this will be in local time, whereas example was generated in
                 * UTC. So for PST we get actually normally hours from 9,10 am.
                 * which would result in impression count of 17 for key 00000,
                 * and 21 ifor key 000001.
                 */
                startTime.add(Calendar.HOUR_OF_DAY, 1);
                endTime.add(Calendar.HOUR_OF_DAY, 3);
                // recalculate the calendars
                startTime.getTimeInMillis();
                endTime.getTimeInMillis();

                /*
                 * same as client2 but print the summaries separately (no
                 * grouping).
                 */

                query.addMeasure("impCnt").addMeasure("click");
                query.addClosedSlice("dim1", ids[0], ids[1]).addGroupBy("dim1");
                query.addHalfOpenSlice("impressionTime", startTime, endTime);

                long ms = System.currentTimeMillis();
                AggregateResultSet rs = query.execute();
                closeables.addFirst(rs);
                while (rs.hasNext()) {
                    rs.next();
                    AggregateResult ar = rs.current();
                    System.out.printf("%032X sum/cnt: impCnt %.4f/%d, click %.4f/%d\n",
                                      new BigInteger(1, (byte[]) ar.getGroupMember("dim1")),
                                      ar.getAggregate("impCnt", "SUM"),
                                      ar.getAggregate("impCnt", "COUNT"),
                                      ar.getAggregate("click", "SUM"),
                                      ar.getAggregate("click", "COUNT"));
                }
                closeables.remove(rs);
                rs.close();

                System.out.printf("query+printout complete in %d ms\n", System.currentTimeMillis() - ms);
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient5(String cubeName) throws IOException, HblException {

        System.out.println("Test5:\n\n");

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /*
             * will try to also constrain for half-open [1:00am,3:00am)
             */

            GregorianCalendar startTime = IOUtil.tryClone(START_BASE);
            GregorianCalendar endTime = IOUtil.tryClone(START_BASE);

            // our actual example generated facts in utc zone of that day.
            startTime.setTimeZone(TimeZone.getTimeZone("UTC"));
            endTime.setTimeZone(TimeZone.getTimeZone("UTC"));

            // flush
            startTime.getTimeInMillis();
            endTime.getTimeInMillis();

            // modify time-of-day-wise
            startTime.add(Calendar.HOUR_OF_DAY, 1);
            endTime.add(Calendar.HOUR_OF_DAY, 3);
            // recalculate the calendars
            startTime.getTimeInMillis();
            endTime.getTimeInMillis();

            PreparedAggregateQuery query = queryClient.createPreparedQuery();

            /*
             * test reuse of the prepared query. Should speedup stuff exactly as
             * prepared query is supposed to do. we also have an option of
             * re-preparing query at any time, but we still need to run reset()
             * to clean out stuff like parameters initialized and execution.
             * reset() does not necessarily cancel previously existing AST tree
             * of the query, only prepare() updates that. but prepare does
             * reset() implicitly, so if we re-prepared the query, the previous
             * parameter set cannot be used.
             */
            long ms = System.currentTimeMillis();
            query.prepare("select dim1, SUM(impCnt) as ?, COUNT(impCnt) as ?, SUM(click) as clickSum, "
                + "COUNT(click) as clickCnt, cannyAvg7d(clickTimeSeries) as ctr " +

                "from Example1 where dim1 in [?] " + ", impressionTime in [?,?) " + ", dim2 in [ '1' ]"
                + "group by dim1");
            System.out.printf("query prepared in %d ms\n", System.currentTimeMillis() - ms);

            for (int i = 0; i < 5; i++) {

                /**
                 * same as client2 but print the summaries separately (no
                 * grouping).
                 * 
                 */
                ms = System.currentTimeMillis();
                query.reset();

                // demo: can parameterize aliases
                // or measure names in the select expression.
                query.setHblParameter(0, "impSum");
                query.setHblParameter(1, "impCnt");

                query.setHblParameter(2, ids[1]);
                // query.setHblParameter(3, ids[1]);
                query.setHblParameter(3, startTime);
                query.setHblParameter(4, endTime);

                // query.addMeasure("impCnt").addMeasure("click");
                // query.addClosedSlice("dim1",ids[0],ids[1]).addGroupBy("dim1");
                // query.addHalfOpenSlice("impressionTime", startTime, endTime);

                AggregateResultSet rs = query.execute();
                closeables.addFirst(rs);
                while (rs.hasNext()) {
                    rs.next();
                    PreparedAggregateResult ar = (PreparedAggregateResult) rs.current();

                    OnlineCannyAvgSummarizer ctrSum = (OnlineCannyAvgSummarizer) ar.getObject("ctr");
                    double wctr = ctrSum == null ? 0 : ctrSum.getValue();

                    System.out.printf("%032X sum/cnt: impCnt %.4f/%d, click %.4f/%d, ctr: %.4f, weighted ctr: %.4f \n",

                                      new BigInteger(1, (byte[]) ar.getObject(0)),
                                      ar.getObject("impSum"),
                                      ar.getObject("impCnt"),
                                      ar.getObject("clickSum"),
                                      ar.getObject("clickCnt"),
                                      (Double) ar.getObject("clickSum") / (Double) ar.getObject("impSum"),
                                      wctr);
                }
                closeables.remove(rs);
                rs.close();

                System.out.printf("query+printout complete in %d ms\n", System.currentTimeMillis() - ms);
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient6(String cubeName) throws IOException, HblException {

        System.out.println("Test6:\n\n");

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            PreparedAggregateQuery query = queryClient.createPreparedQuery();

            /*
             * test reuse of the prepared query. Should speedup stuff exactly as
             * prepared query is supposed to do. we also have an option of
             * re-preparing query at any time, but we still need to run reset()
             * to clean out stuff like parameters initialized and execution.
             * reset() does not necesserily cancel previously existing AST tree
             * of the query, only prepare() updates that. but prepare does
             * reset() implicitly, so if we re-prepared the query, the previous
             * parameter set cannot be used.
             */
            long ms = System.currentTimeMillis();
            query.prepare("select SUM(impCnt) as imp, SUM(click) as click, cannyAvg7d(clickTimeSeries) as wctr7d,"
                + "cannyAvg90d(clickTimeSeries) as wctr90d " + " " + "from Example1 where dim1 in [?]");
            System.out.printf("query prepared in %d ms\n", System.currentTimeMillis() - ms);

            for (int i = 0; i < 3; i++) {

                /*
                 * same as client2 but print the summaries separately (no
                 * grouping).
                 */
                ms = System.currentTimeMillis();
                query.reset();

                query.setHblParameter(0, i);

                AggregateResultSet rs = query.execute();
                closeables.addFirst(rs);
                while (rs.hasNext()) {
                    rs.next();
                    PreparedAggregateResult ar = (PreparedAggregateResult) rs.current();
                    System.out.printf("dim1: %032X impCnt %.4f clickCnt %.4f ctr %.4f wctr 7d %.4f, wctr90d %.4f \n",
                                      i, /*
                                          * new BigInteger(1, (byte[])
                                          * ar.getObject("dim1")),
                                          */
                                      ar.getObject("imp"),
                                      ar.getObject("click"),
                                      (Double) ar.getObject("click") / (Double) ar.getObject("imp"),
                                      ((OnlineCannyAvgSummarizer) ar.getObject(/* 2 */"wctr7d")).getValue(),
                                      ((OnlineCannyAvgSummarizer) ar.getObject("wctr90d")).getValue());
                }
                closeables.remove(rs);
                rs.close();

                System.out.printf("query+printout complete in %d ms\n", System.currentTimeMillis() - ms);
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    /**
     * Month - spanning test
     * 
     * @param cubeName
     * @throws IOException
     * @throws HblException
     */
    private void testClient7(String cubeName) throws IOException, HblException {

        System.out.println("Test7:\n\n");

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /**
             * will try to also constrain for half-open [1:00am,3:00am)
             */

            GregorianCalendar startTime = IOUtil.tryClone(START_BASE);
            GregorianCalendar endTime = IOUtil.tryClone(START_BASE);

            // our actual example generated facts in utc zone of that day.
            startTime.setTimeZone(TimeZone.getTimeZone("UTC"));
            endTime.setTimeZone(TimeZone.getTimeZone("UTC"));

            // flush
            startTime.getTimeInMillis();
            endTime.getTimeInMillis();

            // modify time-of-day-wise
            startTime.add(Calendar.HOUR_OF_DAY, 1);
            endTime.add(Calendar.HOUR_OF_DAY, 3);
            endTime.add(Calendar.MONTH, 9);

            // recalculate the calendars
            startTime.getTimeInMillis();
            endTime.getTimeInMillis();

            PreparedAggregateQuery query = queryClient.createPreparedQuery();

            /*
             * test reuse of the prepared query. Should speedup stuff exactly as
             * prepared query is supposed to do. we also have an option of
             * re-preparing query at any time, but we still need to run reset()
             * to clean out stuff like parameters initialized and execution.
             * reset() does not necessarily cancel previously existing AST tree
             * of the query, only prepare() updates that. but prepare does
             * reset() implicitly, so if we re-prepared the query, the previous
             * parameter set cannot be used.
             */
            long ms = System.currentTimeMillis();
            query.prepare("select dim1, SUM(impCnt) as ?, COUNT(impCnt) as ?, SUM(click) as clickSum, "
                + "COUNT(click) as clickCnt, cannyAvg7d(clickTimeSeries) as ctr " +

                "from Example1 where impressionTime in [?,?), dim1 in [?] " + "group by dim1");
            System.out.printf("query prepared in %d ms\n", System.currentTimeMillis() - ms);

            // warm up helps?
            for (int k = 0; k < 200; k++) {

                for (int i = 0; i < 2; i++) {

                    /**
                     * same as client2 but print the summaries separately (no
                     * grouping).
                     * 
                     */
                    ms = System.currentTimeMillis();
                    query.reset();

                    // demo: can parameterize aliases
                    // or measure names in the select expression.
                    query.setHblParameter(0, "impSum");
                    query.setHblParameter(1, "impCnt");

                    query.setHblParameter(2, startTime);
                    query.setHblParameter(3, endTime);

                    query.setHblParameter(4, i);

                    AggregateResultSet rs = query.execute();
                    closeables.addFirst(rs);
                    while (rs.hasNext()) {
                        rs.next();
                        PreparedAggregateResult ar = (PreparedAggregateResult) rs.current();

                        OnlineCannyAvgSummarizer ctrSum = (OnlineCannyAvgSummarizer) ar.getObject("ctr");
                        Double wctr = ctrSum == null ? null : ctrSum.getValue();

                        System.out
                            .printf("%032X sum/cnt: impCnt %.4f/%d, click %.4f/%d, ctr: %.4f, weighted ctr: %.4f \n",

                                    new BigInteger(1, (byte[]) ar.getObject(0)),
                                    ar.getObject("impSum"),
                                    ar.getObject("impCnt"),
                                    ar.getObject("clickSum"),
                                    ar.getObject("clickCnt"),
                                    (Double) ar.getObject("clickSum") / (Double) ar.getObject("impSum"),
                                    wctr);
                    }
                    closeables.remove(rs);
                    rs.close();

                    System.out.printf("query+printout complete in %d ms\n", System.currentTimeMillis() - ms);
                }
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient8(String cubeName) throws IOException, HblException {

        System.out.println("Test8:\n\n");

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /**
             * will try to also constrain for half-open [1:00am,3:00am)
             */

            GregorianCalendar startTime = IOUtil.tryClone(START_BASE);
            GregorianCalendar endTime = IOUtil.tryClone(START_BASE);

            // our actual example generated facts in utc zone of that day.
            startTime.setTimeZone(TimeZone.getTimeZone("UTC"));
            endTime.setTimeZone(TimeZone.getTimeZone("UTC"));

            // flush
            startTime.getTimeInMillis();
            endTime.getTimeInMillis();

            // modify time-of-day-wise
            startTime.add(Calendar.HOUR_OF_DAY, 1);
            endTime.add(Calendar.HOUR_OF_DAY, 3);
            endTime.add(Calendar.MONTH, 9);

            // recalculate the calendars
            startTime.getTimeInMillis();
            endTime.getTimeInMillis();

            PreparedAggregateQuery query = queryClient.createPreparedQuery();

            /*
             * test reuse of the prepared query. Should speedup stuff exactly as
             * prepared query is supposed to do. we also have an option of
             * re-preparing query at any time, but we still need to run reset()
             * to clean out stuff like parameters initialized and execution.
             * reset() does not necessarily cancel previously existing AST tree
             * of the query, only prepare() updates that. but prepare does
             * reset() implicitly, so if we re-prepared the query, the previous
             * parameter set cannot be used.
             */
            long ms = System.currentTimeMillis();
            query.prepare("select dim1, charDim1, SUM(impCnt) as ?, COUNT(impCnt) as ?, SUM(click) as clickSum, "
                + "COUNT(click) as clickCnt, cannyAvg7d(clickTimeSeries) as ctr " +

                "from Example1 where impressionTime in [?,?), dim1 in [?] " + "group by dim1, charDim1");
            System.out.printf("query prepared in %d ms\n", System.currentTimeMillis() - ms);

                for (int i = 0; i < 2; i++) {

                    /**
                     * same as client2 but print the summaries separately (no
                     * grouping).
                     * 
                     */
                    ms = System.currentTimeMillis();
                    query.reset();

                    // demo: can parameterize aliases
                    // or measure names in the select expression.
                    query.setHblParameter(0, "impSum");
                    query.setHblParameter(1, "impCnt");

                    query.setHblParameter(2, startTime);
                    query.setHblParameter(3, endTime);

                    query.setHblParameter(4, i);

                    AggregateResultSet rs = query.execute();
                    closeables.addFirst(rs);
                    while (rs.hasNext()) {
                        rs.next();
                        PreparedAggregateResult ar = (PreparedAggregateResult) rs.current();

                        OnlineCannyAvgSummarizer ctrSum = (OnlineCannyAvgSummarizer) ar.getObject("ctr");
                        Double wctr = ctrSum == null ? null : ctrSum.getValue();

                        System.out
                            .printf("%032X (charDim=%s) sum/cnt: impCnt %.4f/%d, click %.4f/%d, ctr: %.4f, weighted ctr: %.4f \n",

                                    new BigInteger(1, (byte[]) ar.getObject(0)),
                                    ar.getObject("charDim1"),
                                    ar.getObject("impSum"),
                                    ar.getObject("impCnt"),
                                    ar.getObject("clickSum"),
                                    ar.getObject("clickCnt"),
                                    (Double) ar.getObject("clickSum") / (Double) ar.getObject("impSum"),
                                    wctr);
                    }
                    closeables.remove(rs);
                    rs.close();

                    System.out.printf("query+printout complete in %d ms\n", System.currentTimeMillis() - ms);
                }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private static final int               N          = 24 * 5;
    private static final double            clickRate  = 0.25;
    private static final GregorianCalendar START_BASE = new GregorianCalendar(2011, 8, 1);

    private void simulateInput(FileSystem fs, Path inputDir) throws IOException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();

        byte[] idBytes = new byte[16];

        ByteString[] id = new ByteString[2];
        id[0] = ByteString.copyFrom(idBytes);
        HblUtil.incrementKey(idBytes, 0, idBytes.length);
        id[1] = ByteString.copyFrom(idBytes);

        Random rnd = new Random();

        try {

            // how many months to simulate
            int months = 10;

            Path inpFile = new Path(inputDir, "example1");
            fs.mkdirs(inputDir);
            SequenceFile.Writer w =
                SequenceFile.createWriter(fs, getConf(), inpFile, IntWritable.class, BytesWritable.class);
            closeables.addFirst(w);
            IntWritable iw = new IntWritable();
            BytesWritable bw = new BytesWritable();

            for (int mo = 0; mo < months; mo++) {
                GregorianCalendar start = IOUtil.tryClone(START_BASE);
                start.setTimeZone(TimeZone.getTimeZone("UTC"));
                // flush the cal
                start.getTimeInMillis();
                start.add(Calendar.MONTH, mo);

                for (int i = 0; i < N; i++) {
                    for (int k = 0; k < 2; k++) {
                        for (int j = 0; j < i + k + 1; j++) {
                            CompilerInput.Builder inp = CompilerInput.newBuilder();
                            inp.setDim1(id[k]);
                            inp.setCharDim1("dim1-as-"+(k+1));
                            inp.setDim2(id[k]);
                            inp.setCharDim2("dim2-as-"+(k+1));
                            inp.setDim3(id[k]);
                            inp.setCharDim3("dim3-as-"+(k+1));
                            inp.setImpressionTime(start.getTimeInMillis());
                            inp.setImpCnt(1);
                            inp.setClick(rnd.nextDouble() > clickRate ? 0 : 1);
                            byte[] b = inp.build().toByteArray();
                            bw.set(b, 0, b.length);
                            w.append(iw, bw);
                        }
                    }
                    start.add(Calendar.HOUR_OF_DAY, 1);
                }
            }

        } finally {
            IOUtil.closeAll(closeables);
        }

    }

    private void runScript(String script, Path inputPath) throws IOException {

        try {
            /*
             * this is a pig-version-specific hack to use grunt and its
             * preprocessors in sort of embedded mode. AFAIK it's not official
             * Pig's way to do this
             */
            PigContext pc = new PigContext();

            pc.setExecType(EXEC_TYPE);
            pc.getProperties().setProperty("pig.logfile", "pig.log");
            pc.getProperties().setProperty(PigContext.JOB_NAME, "sample1-compiler-run");

            /*
             * HACK! we probably should get the location of the job jar thru
             * some other way in this example. Since this is just an sample
             * (meaning one's real pipeline framework should figure its own way
             * to configure job jars and kick off pig scripts) and not really
             * part of the real deal, we probably can afford not to go out of
             * our way with this right now.
             */

            File[] jobJars = new File("target").listFiles(new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().matches("sample-\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?-hadoop-job.jar");
                }
            });
            if (jobJars == null || jobJars.length == 0)
                throw new IOException(
                    "hadoop job jar was not found, please rebuild and run from $HBL_HOME/sample location..");

            for (File f : jobJars)
                pc.addJar(f.getAbsolutePath());

            /*
             * pig-preprocess. We specified hbl input as $input in the
             * preambula, so we now need to substitute it using Grunt's
             * preprocessor.
             */

            ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(512);
            StringWriter sw = new StringWriter();
            BufferedReader br = new BufferedReader(new StringReader(script));
            psp.genSubstitutedFile(br, sw, new String[] { "input=" + inputPath }, null);
            sw.close();

            script = sw.toString();
            sw = null;
            br = null;

            Grunt grunt = new Grunt(new BufferedReader(new StringReader(script)), pc);

            int[] codes = grunt.exec();

            int failed = codes[1];
            int succeeded = codes[0];

            System.out.printf("pig jobs failed:%d, pig jobs succeeded:%d.\n", failed, succeeded);

            if (failed != 0)
                throw new IOException("Pig script execution failed, some jobs failed. Check the pig log for errors.");

        } catch (Throwable thr) {
            // sorry, Grunt really declares Throwable to be thrown
            if (thr instanceof IOException)
                throw (IOException) thr;
            throw new IOException(thr);
        }

    }

}
