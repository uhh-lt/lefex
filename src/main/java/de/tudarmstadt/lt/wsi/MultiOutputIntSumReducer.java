package de.tudarmstadt.lt.wsi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutputIntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> mos;
	
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	@Override
	public void reduce(Text keyComposite, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// keyComposite is for example "WF<TAB>word<TAB>feature<TAB>3" or "WW<TAB>word<TAB>20"
		String keyCompositeStr = keyComposite.toString();
		int sepIndex = keyCompositeStr.indexOf('\t');
		String channel = keyCompositeStr.substring(0, sepIndex);
		String key = keyCompositeStr.substring(sepIndex + 1);
		int sum = 0;
		for(IntWritable i : values) {
			sum += i.get();
		}
		mos.write(channel, key, sum);
	}
}