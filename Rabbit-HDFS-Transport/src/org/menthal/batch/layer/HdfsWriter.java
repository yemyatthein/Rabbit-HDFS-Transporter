package org.menthal.batch.layer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class HdfsWriter {

	public static final String ACCEPTOR_PATH = "hdfs://localhost:54310/user/hduser/RabbitHdfs-Workspace/input_buffer";
	public static final String RQ_EXCHANGE_NAME = "event_data";
	public static final String RQ_ROUTING_KEY = "all_event";
	public static final String RQ_HOST = "localhost";
	public static final int INMEMORY_LIMIT = 10;

	private int consumerId;
	private String hdfsFilename;
	private ArrayList<String> eventCollection;

	public HdfsWriter() {
		eventCollection = new ArrayList<String>();
		consumerId = 1; // TODO: to get from param
		hdfsFilename = null;
	}

	public void startConsuming() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(RQ_HOST);
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(RQ_EXCHANGE_NAME, "direct");
			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, RQ_EXCHANGE_NAME, RQ_ROUTING_KEY);

			System.out.println("Waiting for messages on the exchange.");

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				incomingDataHandler(message);
			}
		} catch (IOException | ShutdownSignalException
				| ConsumerCancelledException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void writeToHDFS(ArrayList<String> data) {
		Configuration conf = new Configuration();
		Path path = new Path(ACCEPTOR_PATH + "/" + hdfsFilename);
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

	public void incomingDataHandler(String message) {
		if (hdfsFilename == null) {
			int timestamp = (int) (System.currentTimeMillis() / 1000L);
			hdfsFilename = "data_" + consumerId + "_" + timestamp;
		}
		if (eventCollection.size() < INMEMORY_LIMIT) {
			eventCollection.add(message);
		}
		if (eventCollection.size() == INMEMORY_LIMIT) {
			System.out.println("Time to write to HDFS");
			writeToHDFS(eventCollection);
			hdfsFilename = null;
			eventCollection.clear();
		}
	}

	public static void main(String[] args) throws IOException {
		HdfsWriter hw = new HdfsWriter();
		hw.startConsuming();
		// hw.writeToHDFS(new String[] { "Hi, My name is Ye Myat Thein.",
		// "How are you doing?" });
	}

}
