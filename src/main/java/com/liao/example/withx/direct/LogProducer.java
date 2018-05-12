package com.liao.example.withx.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class LogProducer {
    private final static String EXCHANGE_NAME = "direct_log_ex";
    private final static String[] ROUTE_KEYS = {"info", "warn", "error"};

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
            // 声明转发器类型
            // direct: 直接转发，根据route_key决定
            chl.exchangeDeclare(EXCHANGE_NAME, "direct");
            Random rand = new Random();
            for (int i = 0; i < 20; i++) {
                String routeKey = ROUTE_KEYS[rand.nextInt(3)];
                String msg = routeKey + " log " + new Date().toString();
                // 发送消息到转发器，且符合routeKey的队列上
                chl.basicPublish(EXCHANGE_NAME, routeKey, null, msg.getBytes());
                System.out.println("[LogProducer] send to exchange: " + msg);
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
