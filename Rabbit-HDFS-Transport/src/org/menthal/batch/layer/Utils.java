package org.menthal.batch.layer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Utils {

	public static void writeToHDFS(ArrayList<String> data, String hdfsPath) {
		Configuration conf = new Configuration();
		Path tempPath = new Path(hdfsPath + "_tmp");
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					fs.create(tempPath)));
			for (String item : data) {
				bw.write(item + "\n");
			}
			bw.flush();
			bw.close();
			Path actualPath = new Path(hdfsPath);
			fs.rename(tempPath, actualPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void mergeAndPartition(String srcDir, String destFilename) {
		try {
			MergeAndPartitionJob.runJob(new String[] { srcDir, destFilename });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void copyAndMerge(String[] srcDirs, String destDir) {
		try {
			CopyAndMergeJob.runJob(srcDirs, destDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MergeAndPartitionJob {

		public static class OnlyValueMapper extends
				Mapper<Object, Text, Text, Text> {
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				JSONParser parser = new JSONParser();
				try {
					Object obj = parser.parse(value.toString());
					JSONObject jobj = (JSONObject) obj;
					String groupName = jobj.get("type").toString();
					Text emitValue = new Text(groupName);
					context.write(emitValue, value);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				// context.write(NullWritable.get(), value);
			}
		}

		public static class OnlyValueReducer extends
				Reducer<Text, Text, Text, Text> {
			private MultipleOutputs<Text, Text> mos;

			@Override
			public void setup(Context context) {
				mos = new MultipleOutputs<Text, Text>(context);
			}

			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				Text v = new Text();
				long unixTime = System.currentTimeMillis() / 1000L;
				for (Text val : values) {
					v = val;
					mos.write(key, v, key + "/" + unixTime);
				}
			}

			@Override
			protected void cleanup(Context context) throws IOException,
					InterruptedException {
				mos.close();
			}
		}

		public static void runJob(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("Usage: wordcount <in> <out>");
				System.exit(2);
			}
			Job job = new Job(conf, "word count");
			job.setJarByClass(MergeAndPartitionJob.class);
			job.setMapperClass(OnlyValueMapper.class);
			job.setReducerClass(OnlyValueReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

	private static class CopyAndMergeJob {

		public static class OnlyValueMapper extends
				Mapper<Object, Text, NullWritable, Text> {
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				context.write(NullWritable.get(), value);
			}
		}

		public static class OnlyValueReducer extends
				Reducer<NullWritable, Text, NullWritable, Text> {
			public void reduce(NullWritable key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
				for (Text value : values) {
					context.write(NullWritable.get(), value);
				}
			}
		}

		public static void runJob(String[] inputs, String output)
				throws Exception {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "copy_merge_job");
			job.setJarByClass(CopyAndMergeJob.class);
			job.setMapperClass(OnlyValueMapper.class);
			job.setReducerClass(OnlyValueReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			for (String input : inputs) {
				FileInputFormat.addInputPath(job, new Path(input));
			}
			FileOutputFormat.setOutputPath(job, new Path(output));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

}
