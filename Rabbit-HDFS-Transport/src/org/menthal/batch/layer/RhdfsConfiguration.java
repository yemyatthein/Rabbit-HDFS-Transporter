package org.menthal.batch.layer;

import java.util.HashMap;

public class RhdfsConfiguration {
	
	public static final String TEMP_RB_TO_SNAPSHOT = "temp_data1";
	public static final String SNAPSHOT_TO_NPARTITION = "temp_data2";

	private HashMap<String, String> configurations;

	public RhdfsConfiguration() {
		configurations = new HashMap<String, String>();
		configurations
				.put(Constants.HDFS_BUFFER_PATH,
						"hdfs://localhost:54310/user/hduser/RabbitHdfs-Workspace/input_buffer");
		configurations.put(Constants.HDFS_WORKSPACE_PATH,
				"hdfs://localhost:54310/user/hduser/menthal-workspace");
		configurations.put(Constants.RQ_EXCHANGE, "event_data");
		configurations.put(Constants.RQ_ROUTING_KEY, "all_event");
		configurations.put(Constants.RQ_HOST, "localhost");
		configurations.put(Constants.EVENT_LIMIT, "10");
	}

	public String getString(String key) {
		return configurations.get(key);
	}

	public int getInt(String key) {
		try {
			return Integer.parseInt(configurations.get(key));
		} catch (NumberFormatException e) {
			return Integer.MIN_VALUE;
		}
	}

	public void addConfig(String key, String value) {
		configurations.put(key, value);
	}

	public static void main(String[] args) {
		RhdfsConfiguration conf = new RhdfsConfiguration();
		System.out.println(conf.getString(Constants.HDFS_BUFFER_PATH));
	}
}
