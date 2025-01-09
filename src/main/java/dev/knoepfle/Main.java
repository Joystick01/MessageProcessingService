package dev.knoepfle;

import com.fasterxml.jackson.databind.JsonNode;
import dev.knoepfle.Processor.CoolerMessageMapper;
import dev.knoepfle.Processor.DeduplicationProcessorSupplier;
import dev.knoepfle.Processor.TimestampExtractor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

public class Main {
    public static void main(final String[] args) {

        ConfigurationManager configurationManager = ConfigurationManager.getInstance();

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, configurationManager.getString("APPLICATION_ID_CONFIG"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configurationManager.getString("BOOTSTRAP_SERVERS_CONFIG"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<WindowStore<Integer, Boolean>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("deduplication-store", Duration.ofDays(7), Duration.ofDays(7), false),
            Serdes.Integer(),
            Serdes.Boolean()
        );

        builder.addStateStore(dedupStoreBuilder);

        builder.stream(configurationManager.getString("INPUT_TOPIC"), Consumed.with(Serdes.String(), jsonSerde))
        .mapValues(new CoolerMessageMapper())
        .processValues(new DeduplicationProcessorSupplier(), "deduplication-store")
        .to(configurationManager.getString("OUTPUT_TOPIC"), Produced.with(Serdes.String(), jsonSerde));

        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
        }
    }
}