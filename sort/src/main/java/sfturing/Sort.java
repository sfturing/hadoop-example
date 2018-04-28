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

public class Sort {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line =value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable linenum = new IntWritable(1);
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            System.out.println(key);
            for(IntWritable val:values)
            {
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get()+1);
            }

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
            job.setJarByClass(Sort.class);
            job.setMapperClass(TokenizerMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("input"));
            System.out.println(FileInputFormat.getInputPaths(job)[0].toUri());
            FileOutputFormat.setOutputPath(job, new Path("output"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
