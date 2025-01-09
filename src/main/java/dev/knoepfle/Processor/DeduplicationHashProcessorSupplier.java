package dev.knoepfle.Processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class DeduplicationHashProcessorSupplier  implements ProcessorSupplier<String, JsonNode, Integer, Boolean> {

    @Override
    public Processor<String, JsonNode, Integer, Boolean> get() {
        return new DeduplicationHashProcessor();
    }
}