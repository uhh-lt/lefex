package de.uhh.lt.lefex.Utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class NothingReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

	@Override
	public void reduce(LongWritable lineID, Iterable<Text> texts, Context context)
			throws IOException, InterruptedException {
		for(Text t : texts) {
			context.write(t, NullWritable.get());
		}
	}
}