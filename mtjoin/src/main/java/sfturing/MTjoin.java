package sfturing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MTjoin {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String mapValue;
            String[] records = value.toString().split(" ");
            //如果是表名，忽略
            if (line.contains("factoryname") && line.contains("addressed")){
                return;
            }
            //判断是第一个表还是第二个表，根据id的位置判断。
            if (line.charAt(0) >='0' && line.charAt(0) <= '9'){
                 mapValue = "+";
                for (int i = 1;i < records.length;i++){
                    mapValue = mapValue.concat(records[i]).concat(" ");
                }
                context.write(new Text(String.valueOf(line.charAt(0))),new Text(mapValue));
            }
            if (line.charAt(line.length() - 1) >='0' && line.charAt(line.length() - 1) <= '9'){
                 mapValue = "-";
                 for (int i = 0; i < records.length - 1;i++){
                     mapValue = mapValue.concat(records[i]).concat(" ");
                 }
                 context.write(new Text(String.valueOf(line.charAt(line.length() - 1))),new Text(mapValue));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            context.write(new Text("factoryname"),new Text(("addressname")));
            Text addressname = new Text();
            List<String> factoryname = new ArrayList<String>();
            for (Text value:
                 values) {
                if (value.toString().charAt(0) == '-'){
                    factoryname.add(value.toString().substring(1,value.toString().length()));
                }
                if (value.toString().charAt(0) == '+'){
                    addressname.set(value.toString().substring(1,value.toString().length()));
                }
            }
            for (int i = 0;i < factoryname.size();i++){
                context.write(addressname,new Text(factoryname.get(i)));
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
            job.setJarByClass(MTjoin.class);
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
