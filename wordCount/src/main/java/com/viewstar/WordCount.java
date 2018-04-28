package com.viewstar;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import java.util.*;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
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
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
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
       /* conf.addResource("E:\\hadoop-2.7.3\\etc\\hadoopcore-site.xml");
        conf.addResource("E:\\hadoop-2.7.3\\etc\\hadoophdfs-site.xml");
        conf.addResource("E:\\hadoop-2.7.3\\etc\\hadoopmapred-site.xml");
        conf.addResource("E:\\hadoop-2.7.3\\etc\\hadoopyarn-site.xml");
        conf.set("mapreduce.app-submission.cross-platform", "true");*/
      /*  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //先删除output目录
        deleteDir(conf, otherArgs[otherArgs.length - 1]);*/
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/test.txt"));
        System.out.println(FileInputFormat.getInputPaths(job)[0].toUri());
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}