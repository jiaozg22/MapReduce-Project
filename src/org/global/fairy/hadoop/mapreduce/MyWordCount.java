package org.global.fairy.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {
	// Mapper区域
	/**
	 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * 
	 * 输入key的类型，输入value的类型，输出key 的类型，输出value的类型
	 * 
	 * @author jiao
	 * 
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 获取每行数据的值
			String lineValue = value.toString();
			// 进行分割
			// " \t\n\r\f"
			// 空格 制表符 换行符 \f
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			// 遍历
			while (stringTokenizer.hasMoreTokens()) {
				// 获取每个值
				String wordValue = stringTokenizer.nextToken();
				// 设置map输出的key值
				word.set(wordValue);
				// 上下文输出map的key value值
				context.write(word, one);
			}
		}

	}

	// Reduce区域
	/**
	 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * 
	 * @author jiao
	 * 
	 */
	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(
				Text key,
				Iterable<IntWritable> values,
				org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 用于累加的变量
			int sum = 0;
			// 循环遍历IntWritable
			for (IntWritable value : values) {
				sum += value.get();
			}
			// 设置次数
			result.set(sum);
			context.write(key, result);
		}
	}

	// client区域
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		args = new String[] {
				"hdfs://hadoop-master.dragon.org:9000/opt/data/test3s/dd",
				"hdfs://hadoop-master.dragon.org:9000/opt/data/test3s/out7.data" };

		// 获取配置信息
		Configuration configuration = new Configuration();

		// 优化代码：
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}

		// 创建Job，设置配置信息和job名称
		Job job = new Job(configuration, "wc");

		// 1.设置job运行的类
		job.setJarByClass(MyWordCount.class);

		// 2.设置Mapper和reduce类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		// 3.设置输入文件的目录和输出文件的目录

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 4.设置输出key，value。输出结果的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 5，提交Job，等待运行结果，bean在客户端进行运用剩余余额查询
		boolean isSuccess = job.waitForCompletion(true);
		// 结束
		System.exit(isSuccess ? 0 : 1);
	}

}
