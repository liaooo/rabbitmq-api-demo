package com.liao.example.withx.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class LogProducer {
    private final static String EXCHANGE_NAME = "fanout_log_ex";

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
            // fanout: 广播
            chl.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String msg = "I am message " + new Date().toString();
            // 发送消息到转发器
            chl.basicPublish(EXCHANGE_NAME, "", null, msg.getBytes());
            System.out.println("[LogProducer] send to exchange: " + msg);

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
