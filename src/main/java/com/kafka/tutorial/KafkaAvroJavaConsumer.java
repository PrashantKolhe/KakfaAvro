package com.kafka.tutorial;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroJavaConsumer {

    public static void main(String[] args) {

        Logger logger= LoggerFactory.getLogger(KafkaAvroJavaConsumer.class);

        String bootStrapServers="localhost:9092";
        String schemaRegistryUrl="localhost:8081";

        String groupId="my-third-application";//"customer-consumer-group-v1";
        String topic="customer-avro";

        //create Consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty("schema.registry.url", schemaRegistryUrl);
        properties.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));

        System.out.println("Waiting for data...");

        while (true){
            System.out.println("Polling");
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(1000);

            for (ConsumerRecord<String, Customer> record : records){
                Customer customer = record.value();
                System.out.println(customer);
            }

            kafkaConsumer.commitSync();
        }
    }
}
