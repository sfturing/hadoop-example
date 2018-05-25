package xiaohao.map;

import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String content;
    private static final int MISSING = 9999;

    public void map(LongWritable key, Text value, Context context) {

        //根据文件获取年份和温度。文件格式具体内容见 Hadoop: The Definitive Guide, Fourth Edition内容
        content = value.toString();
        int airTemperature;
        String year = content.substring(15, 19);
        if (content.charAt(87) == '+') {
            airTemperature = Integer.valueOf(content.substring(88, 92));
        } else {
            airTemperature = Integer.valueOf(content.substring(87, 92));
        }
        String quality = content.substring(92,93);
        if (airTemperature != MISSING && quality.matches("[01459]")){
            try {
                System.out.println(year);
                System.out.println(airTemperature);
                context.write(new Text(year),new IntWritable(airTemperature));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
