package cn.sfturing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueStr = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(valueStr);
            while (stringTokenizer.hasMoreTokens()){
               this.word.set(stringTokenizer.nextToken());
               context.write(word,one);
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value :
                    values) {
                int i = value.get();
                sum += i;
            }
            this.result.set(sum);
            context.write(key,result);
        }
    }
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
            if (fs.exists(targetPath)) {
                boolean delResult = fs.delete(targetPath, true);
                if (delResult) {
                    System.out.println(targetPath + " has been deleted sucessfullly.");
                } else {
                    System.out.println(targetPath + " deletion failed.");
                }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        deleteDir(conf, "output");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/test.txt"));
        System.out.println(FileInputFormat.getInputPaths(job)[0].toUri());
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
