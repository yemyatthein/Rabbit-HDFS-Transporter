package org.menthal.batch.layer.rbhdfs;

import org.menthal.batch.layer.RhdfsConfiguration;

public class Runner {

	public static void startTransport(RhdfsConfiguration conf, int numberOfThreads) {
		for (int i = 1; i <= numberOfThreads; i++) {
			new Thread(new RabbitToHdfsConsumer(conf, i)).start();
		}
	}

	public static void main(String[] args) {
		RhdfsConfiguration config = new RhdfsConfiguration();
		Runner.startTransport(config, 1);
	}
}
