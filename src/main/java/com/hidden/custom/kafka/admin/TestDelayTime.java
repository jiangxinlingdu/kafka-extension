package com.hidden.custom.kafka.admin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class TestDelayTime {
    public static void main(String[] args) {
        //Kafka consumer configuration settings
        String topicName = "test2";
        String brokerUrl = "localhost:9092";
        String groupId = "perf-consumer-24493";
        int partition = 1;
        int offset = 0;

        /**  如果消息并没有堆积多少，但是消息已经延迟比如 3 小时了，这个时候需要告警，所以我们需要找到没有消费的 offset id 之后获取一条数据查看时间即可；
         TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
         test2           2          0               333             333             -               -               -
         test2           1          0               334             334             -               -               -
         test2           0          333             333             0               -               -               -
         */
        long lagSeconds = getConsumerLagSecondsByPartition(topicName, brokerUrl, groupId, partition, offset);

        System.out.println(lagSeconds);
    }

    private static long getConsumerLagSecondsByPartition(String topicName, String brokerUrl, String groupId, int partition, int offset) {
        long lagSeconds = 0;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUrl);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        //指定批次消费条数
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        //要发送自定义对象，需要指定对象的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(props);
        consumer.assign(Arrays.asList(new TopicPartition(topicName, partition)));
        //consumer.seekToBeginning(Arrays.asList(new TopicPartition(topicName, 0)));//不改变当前offset
        //不改变当前offset
        consumer.seek(new TopicPartition(topicName, partition), offset);

        /**  如果消息并没有堆积多少，但是消息已经延迟比如 3 小时了，这个时候需要告警，所以我们需要找到没有消费的 offset id 之后获取一条数据查看时间即可；
         TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
         test2           2          0               333             333             -               -               -
         test2           1          0               334             334             -               -               -
         test2           0          333             333             0               -               -               -
         */

        //只获取 offset 第一条数据
        ConsumerRecords<String, Object> records = consumer.poll(10000);
        for (ConsumerRecord<String, Object> record : records) {
            //消息的创建时间
            long timestamp = record.timestamp();
            System.out.println("消息的创建时间：" + timestamp);

            //该消息延时多少秒没有消费了
            lagSeconds = (System.currentTimeMillis() - timestamp) / 1000;
            System.out.println("该消息延时多少秒没有消费了:" + lagSeconds);

            System.out.println(record);
        }

        return lagSeconds;
    }
}
