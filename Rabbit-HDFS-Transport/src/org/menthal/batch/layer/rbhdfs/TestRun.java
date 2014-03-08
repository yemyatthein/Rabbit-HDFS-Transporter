package org.menthal.batch.layer.rbhdfs;

import org.menthal.batch.layer.RhdfsConfiguration;

public class TestRun {
	private static class Merger implements Runnable {
		public void run() {
			for (int i = 0; i < 10; i++) {
				try {
					Thread.sleep(5000);
					
					RhdfsConfiguration config = new RhdfsConfiguration();
					DataStoreAppender dsa = new DataStoreAppender(config);
					dsa.takeSnapshot();
					dsa.snapshotToPartition();
					dsa.appendToMainDataStore();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		RhdfsConfiguration config = new RhdfsConfiguration();
		new Thread(new RabbitToHdfsConsumer(config, 1)).start();
		new Thread(new Merger()).start();
	}
}
