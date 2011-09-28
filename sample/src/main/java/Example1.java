import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;
import org.springframework.core.io.ClassPathResource;

import com.inadco.hbl.compiler.Pig8CubeIncrementalCompilerBean;

public class Example1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Example1(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Pig8CubeIncrementalCompilerBean compiler =
            new Pig8CubeIncrementalCompilerBean(new ClassPathResource("example1.yaml"), new ClassPathResource(
                "example1-preambula.pig"), 5);

        FileSystem dfs = FileSystem.get(getConf());
        Path workPath = new Path(dfs.getWorkingDirectory(), "hbltemp-" + System.currentTimeMillis());
        Path inputPath = new Path(dfs.getWorkingDirectory(), "sample1-input" + System.currentTimeMillis());

        String script = compiler.preparePigSource(workPath.toString());

        runScript(script, inputPath);

        return 0;
    }

    private void runScript(String script, Path inputPath) throws IOException {

        try {
            /*
             * this is a pig-version-specific hack to use grunt and its
             * preprocessors in sort of embedded mode. AFAIK it's not official
             * Pig's way to do this
             */
            PigContext pc = new PigContext();
            pc.setExecType(ExecType.MAPREDUCE); // actually always try
                                                // to use hdfs, just use
                                                // local tracker in
                                                // debug.
            pc.getProperties().setProperty("pig.logfile", "pig.log");
            pc.getProperties().setProperty(PigContext.JOB_NAME, "sample1-compiler-run");

            for (Entry<String, String> entry : getConf())
                pc.getProperties().put(entry.getKey(), entry.getValue());

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
