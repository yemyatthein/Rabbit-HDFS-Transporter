package org.menthal.batch.layer.rbhdfs;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

import org.menthal.batch.layer.Constants;
import org.menthal.batch.layer.RhdfsConfiguration;
import org.menthal.batch.layer.Utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitToHdfsConsumer implements Runnable {

	private RhdfsConfiguration config;
	private int consumerId;
	private String hdfsFilename;
	private ArrayList<String> eventCollection;

	public RabbitToHdfsConsumer(RhdfsConfiguration conf, int cid) {
		eventCollection = new ArrayList<String>();
		consumerId = cid;
		hdfsFilename = null;
		config = conf;
	}

	public void startConsuming() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(config.getString(Constants.RQ_HOST));
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.queueDeclare(config.getString(Constants.RQ_ROUTING_KEY),
					true, false, false, null);
			channel.basicQos(1);

			System.out.println("Waiting for messages on the queue.");

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(config.getString(Constants.RQ_ROUTING_KEY),
					false, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				incomingDataHandler(message);
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		} catch (IOException | ShutdownSignalException
				| ConsumerCancelledException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		startConsuming();
	}

	public void incomingDataHandler(String message) {
		if (hdfsFilename == null) {
			int timestamp = (int) (System.currentTimeMillis() / 1000L);
			String processId = ManagementFactory.getRuntimeMXBean().getName();
			hdfsFilename = "data_" + timestamp + "_" + consumerId + "_"
					+ processId;
		}
		if (eventCollection.size() < config.getInt(Constants.EVENT_LIMIT)) {
			eventCollection.add(message);
		}
		System.out.println(consumerId + " holds " + eventCollection.size()
				+ " events");
		if (eventCollection.size() == config.getInt(Constants.EVENT_LIMIT)) {
			System.out.println("Time to write to HDFS");
			String path = config.getString(Constants.HDFS_BUFFER_PATH) + "/"
					+ hdfsFilename;
			Utils.writeToHDFS(eventCollection, path);
			hdfsFilename = null;
			eventCollection.clear();
		}
	}
}
