package com.liao.example.withoutx;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class Consumer {
    private final static String QUEUE_NAME = "work_queue_persistence";

    public static void main(String[] args) {
        int hashCode = Consumer.class.hashCode();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection conn = null;
        Channel chl = null;
        try {
            conn = factory.newConnection();
            chl = conn.createChannel();
            chl.queueDeclare(QUEUE_NAME, true, false, false, null);
            System.out.println("Consumer is ready");
            // 设置最大服务转发消息数量，设置为1保证当消费者空闲时才分发消息
            // 不设置此参数，默认按Round-Ribbon方式转发消息
            int prefetchCount = 1;
            chl.basicQos(prefetchCount);
            // 创建消费队列
            QueueingConsumer consumer = new QueueingConsumer(chl);
            // 是否开启自动应答
            boolean ack = false;
            chl.basicConsume(QUEUE_NAME, ack, consumer);
            while(true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String msg = new String(delivery.getBody());
                System.out.println("[Consumer{" + hashCode + "}] consume: " + msg);
                work(msg);
                System.out.println("[Consumer{" + hashCode + "}] Done ");
                // 发送消息确认
                chl.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

    private static void work(String msg) {
        try {
            Integer s = Integer.valueOf(String.valueOf(msg.charAt(msg.length() - 1)));
            Thread.sleep(s * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
