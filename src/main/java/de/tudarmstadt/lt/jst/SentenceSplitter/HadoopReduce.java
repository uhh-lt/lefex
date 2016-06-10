package de.tudarmstadt.lt.jst.SentenceSplitter;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReduce extends Reducer<LongWritable, Text, Text, NullWritable> {

    @Override
    public void reduce(LongWritable lineID, Iterable<Text> texts, Context context) throws IOException, InterruptedException {
        for(Text t : texts) {
            context.write(t, NullWritable.get());
            break;
        }
    }
}