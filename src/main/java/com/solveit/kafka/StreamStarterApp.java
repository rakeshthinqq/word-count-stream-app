package com.solveit.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;


import java.util.Arrays;
import java.util.Properties;

public class StreamStarterApp {

    public static void main(String[] args) {
        System.out.println("test");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String, String> stream = kStreamBuilder.stream("word-count-input");

        KTable<String, Long> count = stream.mapValues(str -> str.toLowerCase())
                .flatMapValues(lowerCased -> Arrays.asList(lowerCased.split(" ")))
                .selectKey((ingoredKey, word) -> word)
                .groupByKey()
                .count("Counts");

        count.to(Serdes.String(), Serdes.Long(), "word-count-output");


        KafkaStreams streams = new KafkaStreams(kStreamBuilder, config);
        streams.start();
        System.out.println(stream.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
