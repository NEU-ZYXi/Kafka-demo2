package com.kafka.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        System.out.println("\nExample 1 - new user\n");
        producer.send(userRecord("steven", "First=Steven,Last=Xi,Email=stephenxi@hotmail.com")).get();  // .get() force the Furture to complete
        producer.send(purchaseRecord("steven", "Apple Macbook Pro (1)")).get();

        Thread.sleep(10000);

        System.out.println("\nExample 1 - non existing user\n");
        producer.send(purchaseRecord("john", "Apple Air (1)")).get();

        Thread.sleep(10000);

        System.out.println("\nExample 1 - update user\n");
        producer.send(userRecord("steven", "First=Stephen,Last=Xi,Email=stephenzhenyuan@gmail.com")).get();
        producer.send(purchaseRecord("steven", "Udemy Courses (2)")).get();

        Thread.sleep(10000);

        System.out.println("\nExample 1 - non existing user then user\n");
        producer.send(purchaseRecord("amy", "Oranges (2)")).get();
        producer.send(userRecord("amy", "First=Amy,Last=Yang,Email=amy@hotmail.com")).get() ;
        producer.send(purchaseRecord("amy", "Amazon Books (10)")).get();
        producer.send(userRecord("amy", null)).get();  // delete it for cleanup

        Thread.sleep(10000);

        System.out.println("\nExample 1 - user then delete then data\n");
        producer.send(userRecord("Lee", "First=Lee")).get();
        producer.send(userRecord("Lee", null));
        producer.send(purchaseRecord("Lee", "Think Pad (1)")).get();

        Thread.sleep(10000);

        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key, value);
    }
}
