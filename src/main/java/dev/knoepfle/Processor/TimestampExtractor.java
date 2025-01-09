package dev.knoepfle.Processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;

public class TimestampExtractor implements org.apache.kafka.streams.processor.TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return Instant.parse( ((JsonNode) consumerRecord.value()).get("timestamp").asText()).toEpochMilli();
    }
}
