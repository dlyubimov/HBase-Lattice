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
import com.inadco.hbl.util.HblUtil;
import com.inadco.hbl.util.IOUtil;
import com.inadco.math.aggregators.OnlineCannyAvgSummarizer;

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

    // choose ExecType.LOCAL to debug UDFs
    // private static ExecType EXEC_TYPE = ExecType.LOCAL;
    private static ExecType EXEC_TYPE = ExecType.MAPREDUCE;

    @Override
    public int run(String[] args) throws Exception {

        // script resource
        Resource cubeModelRsrc = new ClassPathResource("example1.yaml");

        // deploy cube schema (optionally dropping the existing one)
        // WARNING: would drop existing cube!!
        HblAdmin hblAdmin = new HblAdmin(cubeModelRsrc);
        hblAdmin.dropCube(getConf());
        hblAdmin.deployCube(getConf());

        String cubeName = hblAdmin.getCube().getName();

        // prepare incremental simulated input
        // and select work dir for the compiler job

        FileSystem dfs = EXEC_TYPE == ExecType.MAPREDUCE ? FileSystem.get(getConf()) : FileSystem.getLocal(getConf());
        Path workPath = new Path(dfs.getWorkingDirectory(), "hbltemp-" + System.currentTimeMillis());
        Path inputPath = new Path(dfs.getWorkingDirectory(), "sample1-input" + System.currentTimeMillis());

        simulateInput(dfs, inputPath);

        // run compiler for the model
        Pig8CubeIncrementalCompilerBean compiler =
            new Pig8CubeIncrementalCompilerBean(getConf(), cubeName, new ClassPathResource("example1-preambula.pig"), 5);
        // test fact compile time exclusion to allow merging different fact
        // stream sources

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

        runScript(script, inputPath);

        testClient1(cubeName);
        testClient2(cubeName);
        testClient3(cubeName);
        testClient4(cubeName);

        // query based tests
        testClient5(cubeName);
        testClient6(cubeName);

        return 0;
    }

    private void testClient1(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            HblQueryClient queryClient = new HblQueryClient(getConf(), cubeName);
            closeables.addFirst(queryClient);

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
            query.addMeasure("impCnt").addMeasure("click");
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
            HblQueryClient queryClient = new HblQueryClient(getConf(), cubeName);
            closeables.addFirst(queryClient);

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

            query.addMeasure("impCnt").addMeasure("click");
            query.addClosedSlice("dim1", ids[0], ids[1]).addGroupBy("dim1");
            AggregateResultSet rs = query.execute();
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
            HblQueryClient queryClient = new HblQueryClient(getConf(), cubeName);
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

            query.addMeasure("impCnt").addMeasure("click");
            query.addClosedSlice("dim1", ids[0], ids[1])/* .addGroupBy("dim1") */;
            AggregateResultSet rs = query.execute();
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
            HblQueryClient queryClient = new HblQueryClient(getConf(), cubeName);
            closeables.addFirst(queryClient);

            AggregateQuery query = queryClient.createQuery();
            for (int i = 0; i < 5; i++) {

                query.reset();
                byte ids[][] = new byte[2][];
                ids[0] = new byte[16];
                ids[1] = new byte[16];
                HblUtil.incrementKey(ids[1], 0, 16);

                /**
                 * will try to also constrain for half-open [1:00am,3:00am)
                 */

                GregorianCalendar startTime = IOUtil.tryClone(START_BASE);
                GregorianCalendar endTime = IOUtil.tryClone(START_BASE);
                startTime.add(Calendar.HOUR_OF_DAY, 1);
                endTime.add(Calendar.HOUR_OF_DAY, 3);
                // recalculate the calendars
                startTime.getTimeInMillis();
                endTime.getTimeInMillis();

                /**
                 * same as client2 but print the summaries separately (no
                 * grouping).
                 * 
                 */

                query.addMeasure("impCnt").addMeasure("click");
                query.addClosedSlice("dim1", ids[0], ids[1]).addGroupBy("dim1");
                query.addHalfOpenSlice("impressionTime", startTime, endTime);

                AggregateResultSet rs = query.execute();
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
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient5(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            HblQueryClient queryClient = new HblQueryClient(getConf(), cubeName);
            closeables.addFirst(queryClient);

            byte ids[][] = new byte[2][];
            ids[0] = new byte[16];
            ids[1] = new byte[16];
            HblUtil.incrementKey(ids[1], 0, 16);

            /**
             * will try to also constrain for half-open [1:00am,3:00am)
             */

            GregorianCalendar startTime = IOUtil.tryClone(START_BASE);
            GregorianCalendar endTime = IOUtil.tryClone(START_BASE);
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
            query.prepare("select dim1, SUM(impCnt) as ?, COUNT(impCnt) as ?, SUM(click) as clickSum, "
                + "COUNT(click) as clickCnt, cannyAvg7d(clickTimeSeries) as ctr " +

                "from Example1 where dim1 in [?] " + ", impressionTime in [?,?) " + ", dim2 in [ '1' ]"
                + "group by dim1");

            System.out.println("Test5:\n\n");

            for (int i = 0; i < 5; i++) {

                /**
                 * same as client2 but print the summaries separately (no
                 * grouping).
                 * 
                 */
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
                                      (Long) ar.getObject("clickCnt") / (Double) ar.getObject("clickSum"),
                                      wctr);
                }
                closeables.remove(rs);
                rs.close();
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private void testClient6(String cubeName) throws IOException, HblException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            HblQueryClient queryClient = new HblQueryClient(getConf(), cubeName);
            closeables.addFirst(queryClient);

            /**
             * will try to also constrain for half-open [1:00am,3:00am)
             */

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
            query.prepare("select " + "SUM(impCnt) as impCnt, " + " dim1 " + "from Example1 group by dim1");

            for (int i = 0; i < 2; i++) {

                /**
                 * same as client2 but print the summaries separately (no
                 * grouping).
                 * 
                 */
                query.reset();

                // query.addMeasure("impCnt").addMeasure("click");
                // query.addClosedSlice("dim1",ids[0],ids[1]).addGroupBy("dim1");
                // query.addHalfOpenSlice("impressionTime", startTime, endTime);

                AggregateResultSet rs = query.execute();
                while (rs.hasNext()) {
                    rs.next();
                    PreparedAggregateResult ar = (PreparedAggregateResult) rs.current();
                    System.out.printf("dim1: %032X impCnt %.4f\n",
                                      new BigInteger(1, (byte[]) ar.getObject("dim1")),
                                      ar.getObject("impCnt"));
                }
                closeables.remove(rs);
                rs.close();
            }

        } finally {
            IOUtil.closeAll(closeables);
        }
    }

    private static final int               N          = 24;
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
            GregorianCalendar start = IOUtil.tryClone(START_BASE);
            start.setTimeZone(TimeZone.getTimeZone("UTC"));
            // flush the cal
            start.getTimeInMillis();

            Path inpFile = new Path(inputDir, "example1");
            fs.mkdirs(inputDir);
            SequenceFile.Writer w =
                SequenceFile.createWriter(fs, getConf(), inpFile, IntWritable.class, BytesWritable.class);
            closeables.addFirst(w);
            IntWritable iw = new IntWritable();
            BytesWritable bw = new BytesWritable();

            for (int i = 0; i < N; i++) {
                for (int k = 0; k < 2; k++) {
                    for (int j = 0; j < i + k; j++) {
                        CompilerInput.Builder inp = CompilerInput.newBuilder();
                        inp.setDim1(id[k]);
                        inp.setDim2(id[k]);
                        inp.setDim3(id[k]);
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
