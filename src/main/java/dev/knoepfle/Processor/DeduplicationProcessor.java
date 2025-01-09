package dev.knoepfle.Processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;

public class DeduplicationProcessor implements FixedKeyProcessor<String, JsonNode, JsonNode> {

    FixedKeyProcessorContext<String, JsonNode> context;
    WindowStore<Integer, Boolean> deduplicationStore;

    @Override
    public void init(FixedKeyProcessorContext<String, JsonNode> context) {
        this.context = context;
        this.deduplicationStore = context.getStateStore("deduplication-store");
    }

    @Override
    public void process(FixedKeyRecord<String, JsonNode> fixedKeyRecord) {

        final int hash = fixedKeyRecord.value().get("deduplicationHash").asInt();
        if (!isDuplicate(hash, fixedKeyRecord.timestamp())) {
            context.forward(fixedKeyRecord);
        }
        deduplicationStore.put(hash, Boolean.TRUE, fixedKeyRecord.timestamp());
    }

    private boolean isDuplicate(final int hash, final long eventTime) {
        final WindowStoreIterator<Boolean> timeIterator = deduplicationStore.fetch(
                hash,
                eventTime - Duration.ofDays(7).toMillis(),
                eventTime + Duration.ofDays(1).toMillis());
        final boolean isDuplicate = timeIterator.hasNext();
        timeIterator.close();
        return isDuplicate;
    }

}

