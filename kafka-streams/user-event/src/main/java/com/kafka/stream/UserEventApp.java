package com.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        KStream<String, String> userPurchasesEnrichedInnerJoin = userPurchases.join(
                usersGlobalTable,
                (key, value) -> key,  // map from (key,value) of stream to key of global table
                (userPurchase, userInfo) -> "Purchase = " + userPurchase + ", User = " + userInfo
        );

        userPurchasesEnrichedInnerJoin.to("user-purchases-enriched-inner-join");

        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(
                usersGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> {
                    if (userInfo != null) {
                        return "Purchase = " + userPurchase + ", User = " + userInfo;
                    } else {
                        return "Purchase = " + userPurchase + ", User = null";
                    }
                }
        );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());
    }
}
