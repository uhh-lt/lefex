package de.tudarmstadt.lt.jst.SentenceSplitter;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(Text sentence, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for(NullWritable v : values) {
            context.write(sentence, NullWritable.get());
            break;
        }
    }
}