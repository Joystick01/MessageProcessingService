package dev.knoepfle.Processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class DeduplicationHashProcessor implements Processor<String, JsonNode, Integer, Boolean> {

    ProcessorContext<Integer, Boolean> context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(Record record) {
        context.forward(new Record<Integer, Boolean>(Integer.valueOf(((JsonNode)record.key()).get("deduplicationHash").asInt()), Boolean.TRUE, record.timestamp(), record.headers()));
    }
}
