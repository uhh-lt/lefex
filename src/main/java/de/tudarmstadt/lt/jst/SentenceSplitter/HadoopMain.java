package de.tudarmstadt.lt.jst.SentenceSplitter;

import de.tudarmstadt.lt.jst.Utils.NothingReducer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Arrays;

public class HadoopMain extends Configured implements Tool {
    public boolean runJob(String inDir, String outDir, boolean makeUnique, boolean compressOutput) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapred.output.compress", compressOutput);
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		Job job = Job.getInstance(conf);
		job.setJarByClass(HadoopMain.class);
		FileInputFormat.addInputPath(job, new Path(inDir));
		FileOutputFormat.setOutputPath(job, new Path(outDir));

		job.setMapperClass(HadoopMap.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		if (makeUnique) job.setReducerClass(HadoopReduce.class);
        else job.setReducerClass(NothingReducer.class);
		job.setJobName("lefex: split by sentences and make them uniq.");
		return job.waitForCompletion(true);
	}

	public int run(String[] args) throws Exception {
		System.out.println("args:" + Arrays.asList(args));
		if (args.length != 4) {
			System.out.println("Outputs one sentence per line and drops too long sentences" +
					" (e.g. as they an cause parsing errors).");
			System.out.println("Usage: <input-corpus> <output-corpus> <unique-sentences> <compress>");
			System.exit(1);
		}
		String inDir = args[0];
		String outDir = args[1];
        boolean makeUnique = Boolean.parseBoolean(args[2]);
		boolean compressOutput = Boolean.parseBoolean(args[3]);
        FileUtils.deleteDirectory(new File(outDir));

        System.out.println("Input text: " + inDir);
        System.out.println("Output directory: " + outDir);
        System.out.println("Unique sentences: " + makeUnique);
		System.out.println("Compress output: " + compressOutput);

        boolean success = runJob(inDir, outDir, makeUnique, compressOutput);
		return success ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new HadoopMain(), args);
		System.exit(res);
	}
}