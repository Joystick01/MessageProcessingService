package dev.knoepfle;

import com.fasterxml.jackson.databind.JsonNode;
import dev.knoepfle.Processor.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import javax.annotation.processing.Processor;
import java.time.Duration;
import java.util.Properties;

public class Main {
    public static void main(final String[] args) throws Exception {


        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("deduplication-store", Duration.ofDays(7), Duration.ofDays(7), false),
            Serdes.Integer(),
            Serdes.Boolean()
        );

        builder.addStateStore(dedupStoreBuilder);

        builder.stream("test", Consumed.with(Serdes.String(), jsonSerde))
        .mapValues(new CoolerMessageMapper())
        .processValues(new DeduplicationProcessorSupplier(), "deduplication-store")
        .to("test-output", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}