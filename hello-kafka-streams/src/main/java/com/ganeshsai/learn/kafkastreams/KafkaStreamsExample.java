package com.ganeshsai.learn.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * This example illustrates how the Kafka-Streams work
 *
 * This example assumes you already have an Apache Kafka-broker and Apache-Zookeeper is up and running
 *  Use the below commands to create the topics
 *  *  kafka-topics --create --topic console --bootstrap-server localhost:9092
 *     kafka-topics --create --topic <your topic-name> --bootstrap-server localhost:9092
 *
 *  Use the below commands to publish the message to topic
 *   * cat messages | kafka-console-producer --topic console --bootstrap-server localhost:9092
 *
 *  Use the below commands to listen for the topics in the console
 *   * kafka-console-consumer --topic  word-count --from-beginning --bootstrap-server localhost:9092 --property print.key=true --value-deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 */
public class KafkaStreamsExample {

//    static final String INPUT_TOPIC = "streams-plain-text-input";
    static final String INPUT_TOPIC = "console";

//    static final String OUTPUT_TOPIC = "streams-exclamation-text-output";
    static final String OUTPUT_TOPIC_EXCLAMATED = "exclamated";
    static  final String OUTPUT_TOPIC_COUNT = "word-count";

    public static void main(String[] args) {
        final String bootStrapServers = args.length >0? args[0]: "localhost:9092";

        // Configure the Streams application
        final Properties streamConfiguration = getStreamsConfiguration(bootStrapServers, "word-count-example");

        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfiguration);

        streams.cleanUp();
        streams.start();

        final StreamsBuilder builder1 = new StreamsBuilder();
        createExclamationStream(builder1);

        final Properties streamConfiguration1 = getStreamsConfiguration(bootStrapServers, "exclamation-example");
        final KafkaStreams streams1 = new KafkaStreams(builder1.build(), streamConfiguration1);
        streams1.cleanUp();
        streams1.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(streams1::close));

    }

    private static void createExclamationStream(StreamsBuilder builder) {
        final KStream<String, String> text = builder.stream(INPUT_TOPIC);

        final KStream<String, String> exclamation = text.mapValues(value-> value + "!!");

        exclamation.to(OUTPUT_TOPIC_EXCLAMATED);

    }

    private static Properties getStreamsConfiguration(String bootStrapServers, String applicationId) {
        final Properties streamConfiguration = new Properties();

        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG,"word-count-example-client");
        streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "temptestpath");
        return streamConfiguration;
    }

    /**
     *  This methods will read the text from the console and print the number of occurrence of each word
     * @param builder streambuilder/pipeline builder
     */
    private static void createWordCountStream(StreamsBuilder builder) {
        final KStream<String, String> text = builder.stream(INPUT_TOPIC);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = text
                // Take text from the console/INPUT_TOPIC
                // Split the based on whitespace to words.
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                // Group the split data basing on the word
                .groupBy((keyIgnored, word) -> word)
                // Count the occurrences of the each word
                .count();
        // Write the KTable<String, Long> (which is essentially a topic)
        wordCounts.toStream().to(OUTPUT_TOPIC_COUNT, Produced.with(Serdes.String(), Serdes.Long()));

    }
}
