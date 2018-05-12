package com.liao.example.withx.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class LogFileConsumer {
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
            // 绑定一个非持久的、唯一的、自动删除的队列
            // 即设置该通道在转发器上所关注的队列
            String queueName = chl.queueDeclare().getQueue();
            chl.queueBind(queueName, EXCHANGE_NAME, "");
            // 创建一个消费者
            QueueingConsumer consumer = new QueueingConsumer(chl);
            // 是否开启自动应答
            boolean ack = false;
            // 设置通道的消费者
            chl.basicConsume(queueName, ack, consumer);
            System.out.println("LogFileConsumer is ready");
            while(true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String msg = new String(delivery.getBody());
                System.out.println("[LogFileConsumer] consume: " + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

}
