package com.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // create a stream from Kafka
        KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input");

        // map values to lowercase
        // flatMap values to split by space
        // select key to apply a key
        // group by key to aggregate
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split("\\s+")))
                .selectKey((initialKey, word) -> word)
                .groupByKey()
                .count(Materialized.as("counts"));

//        wordCounts.toStream().foreach((w, c) -> System.out.println(w + ": " + c));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // close the application with shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

// bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic word-count-output \
// --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true \
// --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
// --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
