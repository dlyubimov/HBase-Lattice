import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.PreparedAggregateResult;
import com.inadco.hbl.mapreduce.HblInputFormat;
import com.inadco.hbl.mapreduce.HblMapper;

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

/**
 * A demo of a locality-optimized map-reduce job that runs on the result of an
 * HBL query.
 * <P>
 * 
 * (this assumes the data is compiled and prepared by running Example1.java). To
 * run, do
 * 
 * <pre>
 *   hadoop jar sample/target/sample-0.2.10-SNAPSHOT-hadoop-job.jar MRExample1Query
 * </pre>
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class MRExample1Query extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job mr = new Job(getConf(), "HBL MR Query Example");

        /*
         * input
         */
        mr.setInputFormatClass(HblInputFormat.class);
        HblInputFormat.setHblQuery(mr, "select charDim1,SUM(impCnt) as impCnt, "
            + "SUM(click) as clickCnt from Example1 group by charDim1");

        /*
         * output
         */
        mr.setOutputFormatClass(TextOutputFormat.class);

        /*
         * just for convenience drop our output from previous runs so we don't
         * have to dropit manually. Or the job will not start.
         */
        Path p = new Path("/tmp/MRExample1");
        FileSystem dfs = FileSystem.get(getConf());
        if (dfs.exists(p))
            dfs.delete(p, true);

        FileOutputFormat.setOutputPath(mr, new Path(p, "out"));

        /*
         * mapper
         */
        mr.setMapperClass(MRExample1Mapper.class);
        mr.setMapOutputKeyClass(NullWritable.class);
        mr.setMapOutputValueClass(Text.class);

        /*
         * reducer -- we don't use any here.
         */
        mr.setNumReduceTasks(0);

        /*
         * job classpath
         */
        mr.setJarByClass(MRExample1Query.class);

        /*
         * go
         */
        mr.submit();
        boolean result = mr.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public static final class MRExample1Mapper extends HblMapper<NullWritable, Text> {

        private final Text val = new Text();

        @Override
        protected void map(NullWritable key, PreparedAggregateResult par, Context context) throws IOException,
            InterruptedException {
            try {

                /*
                 * Just write tab-separated lines -- group, impCnt, clickCnt.
                 */
                val.set(String.format("%s\t%s\t%s", par.getObject("charDim1").toString(), par.getObject("impCnt")
                    .toString(), par.getObject("clickCnt").toString()));

                context.write(NullWritable.get(), val);

            } catch (HblException exc) {
                throw new IOException(exc);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MRExample1Query(), args);
    }

}
