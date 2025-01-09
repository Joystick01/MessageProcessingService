package dev.knoepfle.Processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;

public class DeduplicationProcessorSupplier implements FixedKeyProcessorSupplier<String, JsonNode, JsonNode> {
    @Override
    public FixedKeyProcessor<String, JsonNode, JsonNode> get() {
        return new DeduplicationProcessor();
    }
}
