package de.tudarmstadt.lt.jst.CoNLL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Arrays;

public class HadoopMain extends Configured implements Tool {
    public boolean runJob(String inDir, String outDir) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean("mapred.output.compress", false);
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        Job job = Job.getInstance(conf);
        job.setJarByClass(HadoopMain.class);
        FileInputFormat.addInputPath(job, new Path(inDir));
        FileOutputFormat.setOutputPath(job, new Path(outDir));
        job.setMapperClass(HadoopMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setJobName("lefex: CoNLL");
        return job.waitForCompletion(true);
    }

    public int run(String[] args) throws Exception {
        System.out.println("args:" + Arrays.asList(args));
        if (args.length != 2) {
            System.out.println("Usage: <input-corpus> <output-conll-corpus>");
            System.out.println("<input-corpus>\tA text corpus to parse.");
            System.out.println("<output-conll-corpus>\tA csv file with the dependency-parsed corpus in the CoNLL format.");
            System.exit(1);
        }
        String inDir = args[0];
        String outDir = args[1];
        boolean success = runJob(inDir, outDir);
        return success ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new HadoopMain(), args);
        System.exit(res);
    }
}
