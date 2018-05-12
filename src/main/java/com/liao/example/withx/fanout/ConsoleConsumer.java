package com.liao.example.withx.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class ConsoleConsumer {
    private final static String EXCHANGE_NAME = "fanout_log_ex";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection conn = null;
        Channel chl = null;
        try {
            conn = factory.newConnection();
            chl = conn.createChannel();
            chl.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String queueName = chl.queueDeclare().getQueue();
            chl.queueBind(queueName, EXCHANGE_NAME, "");
            QueueingConsumer consumer = new QueueingConsumer(chl);
            boolean ack = false;
            chl.basicConsume(queueName, ack, consumer);
            System.out.println("ConsoleConsumer is ready");
            while(true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String msg = new String(delivery.getBody());
                System.out.println("[ConsoleConsumer] consume: " + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

}
