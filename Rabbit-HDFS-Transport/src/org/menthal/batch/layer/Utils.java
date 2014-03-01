package org.menthal.batch.layer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
	
}
