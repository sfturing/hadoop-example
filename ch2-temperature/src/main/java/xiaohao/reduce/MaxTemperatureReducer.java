package xiaohao.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> value,
                       Context context
    ) throws IOException, InterruptedException {
        Integer maxValue = Integer.MIN_VALUE;
        for (IntWritable val:
                value){
            maxValue = Math.max(maxValue,val.get());
        }
        context.write(new Text(key),new IntWritable(maxValue));

    }
}
