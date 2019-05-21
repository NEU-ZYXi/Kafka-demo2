package com.kafka.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);  // exactly once semantic
        final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions = builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonNodeSerde));

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.with(Serdes.String(), jsonNodeSerde)
                );

        bankBalance.toStream().to("bank-balance", Produced.with(Serdes.String(), jsonNodeSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
