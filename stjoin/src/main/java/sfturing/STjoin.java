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
import java.util.ArrayList;
import java.util.List;

public class STjoin {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            String child = values[0];
            String parent = values[1];
            //将child和parent放入上下文。-代表正向。+代表逆向。
            /*
            可以这样理解：
            有如下表： child parent grandparent
                        +     key   -
             将key值代表parent，+value是child -value是grandparent。
             这样一个reduce就可以判断爷孙关系
            */
            context.write(new Text(child),new Text("-" + parent));
            context.write(new Text(parent),new Text(("+") + child));

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> child = new ArrayList<String>();
            List<String> grandparent = new ArrayList<String>();
            //遍历value值，—value是祖父母，+是孩子。
            for (Text value :
                    values) {
                if (value != null && value.toString().contains("-")){
                    grandparent.add(value.toString().substring(1,value.toString().length()));
                }
                if (value != null && value.toString().contains("+")){
                    child.add(value.toString().substring(1,value.toString().length()));
                }
            }
            //如果child和grandparent有值，则证明有关系。
            if (!child.isEmpty() && !grandparent.isEmpty()){
                for (int i = 0;i < child.size();i++){
                    Text childTmp = new Text(child.get(i));
                    for (int j = 0;j < grandparent.size();j++){
                        //System.out.println(childTmp.toString()+" -----"+ grandparent.get(j));
                        context.write(childTmp,new Text(grandparent.get(j)));
                    }
                }
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
            job.setJarByClass(STjoin.class);
            job.setMapperClass(TokenizerMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("input"));
            System.out.println(FileInputFormat.getInputPaths(job)[0].toUri());
            FileOutputFormat.setOutputPath(job, new Path("output"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
