package org.menthal.batch.layer.rbhdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.menthal.batch.layer.Constants;
import org.menthal.batch.layer.RhdfsConfiguration;
import org.menthal.batch.layer.Utils;

public class DataStoreAppender {

	private RhdfsConfiguration config;

	public DataStoreAppender(RhdfsConfiguration conf) {
		this.config = conf;
	}

	public void takeSnapshot() {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);

			Path srcDirPath = new Path(
					config.getString(Constants.HDFS_BUFFER_PATH));
			Path destDirPath = new Path(
					config.getString(Constants.HDFS_WORKSPACE_PATH) + "/"
							+ RhdfsConfiguration.TEMP_RB_TO_SNAPSHOT);
			FileStatus[] fsa = fs.listStatus(srcDirPath);
			fs.mkdirs(destDirPath);
			for (int i = 0; i < fsa.length; i++) {
				if (fsa[i].getPath().getName().contains(".tmp")) {
					continue;
				}
				Path tempDestFilePath = new Path(destDirPath.toString() + "/"
						+ fsa[i].getPath().getName());
				FileUtil.copy(fs, fsa[i].getPath(), fs, tempDestFilePath, true,
						conf);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void snapshotToPartition() {
		String srcDir = config.getString(Constants.HDFS_WORKSPACE_PATH) + "/"
				+ RhdfsConfiguration.TEMP_RB_TO_SNAPSHOT;
		String destFile = config.getString(Constants.HDFS_WORKSPACE_PATH) + "/"
				+ "_yemyat_";
		Utils.mergeAndPartition(srcDir, destFile);
	}

	public static void main(String[] args) throws Exception {
		RhdfsConfiguration config = new RhdfsConfiguration();
		DataStoreAppender dsa = new DataStoreAppender(config);
		//dsa.takeSnapshot();
		dsa.snapshotToPartition();
	}

}
