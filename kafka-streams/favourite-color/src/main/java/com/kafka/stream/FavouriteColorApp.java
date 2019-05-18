package com.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // disable the cache to demonstrate all the steps
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("favourite-color-input");

        KStream<String, String> usersAndColors = textLines
                // filter bad data
                .filter((key, value) -> value.contains(","))
                // select a key which is the user id
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // get the color from the data
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // filter undesired color
                .filter((user, color) -> Arrays.asList("green", "red", "blue").contains(color));

        usersAndColors.to("user-keys-and-colors");

        KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-colors");
        KTable<String, Long> favouriteColors = usersAndColorsTable
                // group by color within table
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Materialized.as("CountsByColors"));

        favouriteColors.toStream().to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
