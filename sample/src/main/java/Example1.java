import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
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
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.compiler.Pig8CubeIncrementalCompilerBean;
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

        // runjarArgs[0]="target/sample-0.1.0-SNAPSHOT-hadoop-job.jar";

        ToolRunner.run(new Example1(), args);

    }

    @Override
    public int run(String[] args) throws Exception {

        // script resource
        Resource cubeModelRsrc = new ClassPathResource("example1.yaml");

        FileSystem dfs = FileSystem.get(getConf());
        Path workPath = new Path(dfs.getWorkingDirectory(), "hbltemp-" + System.currentTimeMillis());
        Path inputPath = new Path(dfs.getWorkingDirectory(), "sample1-input" + System.currentTimeMillis());

        // prepare incremental simulated input

        simulateInput(dfs, inputPath);

        // make sure hbase schema is rolled out
        HblAdmin hblAdmin = new HblAdmin(cubeModelRsrc);
        hblAdmin.dropCube(getConf());
        hblAdmin.deployCube(getConf());

        // run compiler for the model
        Pig8CubeIncrementalCompilerBean compiler =
            new Pig8CubeIncrementalCompilerBean(cubeModelRsrc, new ClassPathResource("example1-preambula.pig"), 5);

        String script = compiler.preparePigSource(workPath.toString());

        runScript(script, inputPath);

        return 0;
    }

    private static final int    N         = 100;
    private static final double clickRate = 0.25;

    private void simulateInput(FileSystem fs, Path inputDir) throws IOException {
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();

        byte[] idBytes = new byte[16];

        ByteString[] id = new ByteString[2];
        id[0] = ByteString.copyFrom(idBytes);
        HblUtil.incrementKey(idBytes, 0, idBytes.length);
        id[1] = ByteString.copyFrom(idBytes);

        Random rnd = new Random();

        try {
            GregorianCalendar start = new GregorianCalendar(2011, 8, 1);
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
                for (int j = 0; j < i; j++) {
                    for (int k = 0; k < 2; k++) {
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

            pc.setExecType(ExecType.MAPREDUCE);
            pc.getProperties().setProperty("pig.logfile", "pig.log");
            pc.getProperties().setProperty(PigContext.JOB_NAME, "sample1-compiler-run");

            pc.addJar("target/sample-0.1.0-SNAPSHOT-hadoop-job.jar");

            Configuration conf = getConf();

            FileSystem dfs = FileSystem.get(conf);

//            Path jobPath = new Path(dfs.getWorkingDirectory(), "hbl-job.jar");
//
//            dfs.copyFromLocalFile(false, true, new Path("target/sample-0.1.0-SNAPSHOT-hadoop-job.jar"), jobPath);
//            DistributedCache.addArchiveToClassPath(jobPath, conf, dfs);
//            
//            conf.set("mapred.job.classpath.archives", jobPath.toString());

//            for (Entry<String, String> entry : conf)
//                pc.getProperties().put(entry.getKey(), entry.getValue());

            // pig-preprocess
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
