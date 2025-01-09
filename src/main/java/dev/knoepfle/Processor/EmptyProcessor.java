package dev.knoepfle.Processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class EmptyProcessor implements Processor<Object, Object, Void, Void> {

    ProcessorContext<Void, Void> context;
    private ReadOnlyKeyValueStore<String, String> globalStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        this.globalStore = (ReadOnlyKeyValueStore<String, String>) context.getStateStore("deduplication-store");
    }

    @Override
    public void process(Record<Object, Object> record) {
        globalStore.all().forEachRemaining(windowStoreIterator -> {
            System.out.println("deduplicationHashListITEMS: " + windowStoreIterator.key);
        });
    }
}
