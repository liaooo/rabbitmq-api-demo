package com.liao.example.withoutx;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {
    private final static String QUEUE_NAME = "work_queue_persistence";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection conn = null;
        Channel chl = null;
        try {
            // 创建连接
            conn = factory.newConnection();
            // 创建通道
            chl = conn.createChannel();
            // 声明队列
            // 设置队列是否持久化
            boolean durable = true;
            chl.queueDeclare(QUEUE_NAME, durable, false, false, null);
            for (int i = 5; i > 0; i--) {
                String msg = "I am message " + i;
                // 发送消息，并声明消息需要持久化
                // 第一个参数是转发器，第二个是routeKey，""是默认的转发器，此时根据routeKey决定发送到哪个队列
                chl.basicPublish("",
                        QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        msg.getBytes());
                System.out.println("[Producer] product: " + msg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } finally {
            if (chl != null) {
                try {
                    chl.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
