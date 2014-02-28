package org.menthal.batch.layer;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class HdfsWriter {

	public void start() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare("ymt_exchange", "direct");
			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, "ymt_exchange", "red");

			System.out.println("Waiting for messages on the exchange.");

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				String routingKey = delivery.getEnvelope().getRoutingKey();
				System.out.println("Received data from #" + routingKey + "# => " + message);
			}
		} catch (IOException | ShutdownSignalException
				| ConsumerCancelledException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		HdfsWriter hw = new HdfsWriter();
		hw.start();
	}

}
