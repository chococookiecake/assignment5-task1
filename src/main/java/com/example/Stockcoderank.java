import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Stockcoderank {

    // 任务1的第1个Mapper用于统计股票代码的出现
    public static class StockCodeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stockCode = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 4) {
                String stockCodeStr = fields[3].trim(); // 提取股票代码
                stockCode.set(stockCodeStr);
                context.write(stockCode, one); // 输出 (股票代码, 1)
            }
        }
    }

    // 任务1的第1个Reducer用于累加每个股票代码出现次数
    public static class StockCodeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // 累加每个股票代码的出现次数
            }
            result.set(sum); // 设置结果
            context.write(key, result); // 输出 (股票代码, 出现次数)
        }
    }

    // 按照出现次数降序排序
    public static class DescendingCountComparator extends WritableComparator{
        protected DescendingCountComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            // 按照出现次数降序排序
            IntWritable val1 = (IntWritable) w1;
            IntWritable val2 = (IntWritable) w2;
            return val2.compareTo(val1);  // 降序排序
        }
    }

    public static class SkipFirstLineInputFormat extends TextInputFormat {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new SkipFirstLineRecordReader();  // 返回一个符合要求的 RecordReader
        }

        // 静态的 RecordReader 类，继承 LineRecordReader
        public static class SkipFirstLineRecordReader extends LineRecordReader {
            private boolean firstLineSkipped = false;

            @Override
            public boolean nextKeyValue() throws IOException {
                // 跳过第一行
                if (!firstLineSkipped) {
                    firstLineSkipped = true;
                    // 调用一次 nextKeyValue 跳过第一行
                    super.nextKeyValue();  
                    return super.nextKeyValue();  // 返回第二行及以后的内容
                } else {
                    return super.nextKeyValue();  // 正常处理
                }
            }
        }
    }

    //任务1的第2个Mapper用于将股票代码，出现次数反过来
    public static class RankMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();  // 出现次数
        private Text stockCode = new Text();  // 股票代码

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 假设输入为: 股票代码,出现次数
            String[] fields = value.toString().split("[,\\s]+");
            
            if (fields.length == 2) {
                String stockCodeStr = fields[0].trim();  // 提取股票代码
                int occurrence = Integer.parseInt(fields[1].trim());  // 提取出现次数
                
                stockCode.set(stockCodeStr);  // 设置股票代码
                count.set(occurrence);  // 设置出现次数
                
                // 输出 (出现次数, 股票代码)
                context.write(count, stockCode);
            }
        }
    }
 
    // 任务1的第2个Reducer用于计算排名 
    public static class RankReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private IntWritable result = new IntWritable();
        private int rank = 1;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key 是出现次数，values 是相同出现次数的股票代码
            for (Text stock : values) {
                result.set(key.get());  // 设置出现次数
                context.write(new Text(rank + ":" + stock.toString() + "," + result.toString()),NullWritable.get());  // 输出排名、股票代码、出现次数
                rank++;  // 排名++
            }
        }
    }

    // 主函数，配置 MapReduce 作业
    public static void main(String[] args) throws Exception {
        // 任务1的第1个MapReduce作业：用于统计股票代码出现次数
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Stock Code Count");
        job1.setJarByClass(Stockcoderank.class);
        job1.setMapperClass(StockCodeMapper.class);
        job1.setCombinerClass(StockCodeReducer.class);  // 使用 Combiner 来减少数据量
        job1.setReducerClass(StockCodeReducer.class);
        job1.setOutputKeyClass(Text.class);  // 输出的 Key 是 股票代码
        job1.setOutputValueClass(IntWritable.class);  // 输出的 Value 是 1 （出现次数）
        job1.setInputFormatClass(SkipFirstLineInputFormat.class);

        // 输入路径和输出路径
        FileInputFormat.addInputPath(job1, new Path(args[0]));  // 作业1的输入路径
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/codecnttmp"));  // 作业1的输出路径
        
        // 执行作业1，等待作业1完成
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // 任务1的第2个MapReduce作业：用于排序
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort Stock Code");
        job2.setJarByClass(Stockcoderank.class);
        job2.setMapperClass(RankMapper.class);
        job2.setReducerClass(RankReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);   // 键: 出现次数
        job2.setMapOutputValueClass(Text.class);        // 值: 股票代码
        job2.setOutputKeyClass(Text.class);   // 键: 排名、股票代码、出现次数
        job2.setOutputValueClass(NullWritable.class); // 值: NullWritable
        job2.setSortComparatorClass(DescendingCountComparator.class);
 
        // 作业2的输入路径是作业1的输出路径
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/codecnttmp/part-*"));  // 作业1的输出作为作业2的输入路径
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/coderank"));  // 作业2的输出路径
        
        // 执行作业2，等待作业2完成
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        
    }   
}