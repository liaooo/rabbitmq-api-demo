package com.liao.example.withx.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class WarnConsumer {
    public final static String EXCHANGE_NAME = "direct_log_ex";
    private final static String ROUTE_KEY = "warn";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection conn = null;
        Channel chl = null;
        try {
            conn = factory.newConnection();
            chl = conn.createChannel();
            chl.exchangeDeclare(EXCHANGE_NAME, "direct");
            String queueName = chl.queueDeclare().getQueue();
            chl.queueBind(queueName, EXCHANGE_NAME, ROUTE_KEY);
            chl.queueBind(queueName, EXCHANGE_NAME, "error");
            QueueingConsumer consumer = new QueueingConsumer(chl);
            boolean ack = false;
            chl.basicConsume(queueName, ack, consumer);
            System.out.println(ROUTE_KEY + "-Consumer is ready");
            while(true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String msg = new String(delivery.getBody());
                System.out.println("received: " + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}
