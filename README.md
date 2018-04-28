# hadoop-example
hadoop入门例子实践

# wordCount：
MAP阶段：使用StringTokenizer 将一行String分离成不同的单词，输出<KEY,VALUE>, 例如<WORD,1>
REDUCE阶段：<KEY,VALUE-LIST> 例子如下<WORD,<1,1,1,1,1,.....>> 将VALUE的值进行相加，输出结果

# remove duplication 
MAP阶段：

MAP阶段：将数据源的VALUE作为key输出，VALUE随意<KEY,VALUE>

REDUCE阶段：因为是去除重复，<KEY,<VALUE,VALUE,VALUE...>>, 将以上的数据源只保留KEY值进行输出即可。

#Sort
MAP阶段：将数据进行读取，输出key值为IntWritable,hadoop会对其进行排序。输出的value为1.目的是出现重复的数字。
REDUCE阶段： 读取value-list，有几个1输出几次，定义全局变量lineNumber，每一次context.write(key,value),lineNumber加一。
有个疑问是  job.setCombinerClass(),不设置，如果使用竟然会出问题。我还没有解决这件事。等我多学学回头解决这个问题。