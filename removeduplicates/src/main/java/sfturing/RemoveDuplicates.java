package sfturing;

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

public class RemoveDuplicates {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private static IntWritable NULL=new IntWritable(0);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            this.word.set(value);
            context.write(value,NULL);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text result = new Text();
        private static IntWritable NULL=new IntWritable(0);
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            this.result.set(key);
            context.write(result,NULL);
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
            job.setJarByClass(RemoveDuplicates.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("input"));
            System.out.println(FileInputFormat.getInputPaths(job)[0].toUri());
            FileOutputFormat.setOutputPath(job, new Path("output"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
