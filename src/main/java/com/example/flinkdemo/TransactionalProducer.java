package com.example.flinkdemo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionalProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_transactional_id"); // 定义事务ID

        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        // 初始化事务
        producer.initTransactions();
        producer.beginTransaction();

        try {
            // 在同一个事务中发送消息和更新点位
            producer.send(new ProducerRecord<>("my_topic", "my_key", "my_value"));
            updatePosition();  // 更新点位信息

            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            // 回滚事务
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }

    private static void updatePosition() {
        // 更新点位信息的逻辑
    }
}
