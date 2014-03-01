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
		Path path = new Path(hdfsPath);
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					fs.create(path)));
			for (String item : data) {
				bw.write(item + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
}
