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
	// Mapper����
	/**
	 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * 
	 * ����key�����ͣ�����value�����ͣ����key �����ͣ����value������
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
			// ��ȡÿ�����ݵ�ֵ
			String lineValue = value.toString();
			// ���зָ�
			// " \t\n\r\f"
			// �ո� �Ʊ�� ���з� \f
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			// ����
			while (stringTokenizer.hasMoreTokens()) {
				// ��ȡÿ��ֵ
				String wordValue = stringTokenizer.nextToken();
				// ����map�����keyֵ
				word.set(wordValue);
				// ���������map��key valueֵ
				context.write(word, one);
			}
		}

	}

	// Reduce����
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
			// �����ۼӵı���
			int sum = 0;
			// ѭ������IntWritable
			for (IntWritable value : values) {
				sum += value.get();
			}
			// ���ô���
			result.set(sum);
			context.write(key, result);
		}
	}

	// client����
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		args = new String[] {
				"hdfs://hadoop-master.dragon.org:9000/opt/data/test3s/dd",
				"hdfs://hadoop-master.dragon.org:9000/opt/data/test3s/out7.data" };

		// ��ȡ������Ϣ
		Configuration configuration = new Configuration();

		// �Ż����룺
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}

		// ����Job������������Ϣ��job����
		Job job = new Job(configuration, "wc");

		// 1.����job���е���
		job.setJarByClass(MyWordCount.class);

		// 2.����Mapper��reduce��
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		// 3.���������ļ���Ŀ¼������ļ���Ŀ¼

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 4.�������key��value����������key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 5���ύJob���ȴ����н����bean�ڿͻ��˽�������ʣ������ѯ
		boolean isSuccess = job.waitForCompletion(true);
		// ����
		System.exit(isSuccess ? 0 : 1);
	}

}
