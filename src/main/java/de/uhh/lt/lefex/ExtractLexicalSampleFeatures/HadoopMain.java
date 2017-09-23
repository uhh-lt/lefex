package de.uhh.lt.lefex.ExtractLexicalSampleFeatures;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
        FileSystem fs = FileSystem.get(conf);
        String _outDir = outDir;
        int outDirSuffix = 1;
        while (fs.exists(new Path(_outDir))) {
            _outDir = outDir + outDirSuffix;
            outDirSuffix++;
        }
        conf.setBoolean("mapred.output.compress", false);
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        Job job = Job.getInstance(conf);

        job.setJarByClass(HadoopMain.class);
        FileInputFormat.addInputPath(job, new Path(inDir));
        FileOutputFormat.setOutputPath(job, new Path(_outDir));
        job.setMapperClass(HadoopMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setReducerClass(NothingReducer.class);

        job.setJobName("lefex: ExtractLexicalSampleFeatures");
        return job.waitForCompletion(true);
    }

    public int run(String[] args) throws Exception {
        System.out.println("args:" + Arrays.asList(args));
        if (args.length != 2) {
            System.out.println("Usage: <input-lexsample> <output-lexsample-with-features>");
            System.out.println("<input-lexical-sample>\tA csv lexical sample dataset with 9 columns.");
            System.out.println("<output-lexical-samplewith-features>\tA csv lexical sample dataset with 12 columns" +
                    ": 3 extra columns store features extracted from the context ''.");
            System.out.println("This class takes the following paramemeters via the MapReduce options e.g. -D:");
            System.out.println("holing.type\t'dependency' or 'trigram' or 'dependency+trigram'. Default -- 'dependency'");
            System.out.println("holing.dependencies.semantify\ttrue or false .Default --  true");
            System.out.println("holing.lemmatize\ttrue or false. Default -- true");
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
