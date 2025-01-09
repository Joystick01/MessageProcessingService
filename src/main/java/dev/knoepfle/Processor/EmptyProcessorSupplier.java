package dev.knoepfle.Processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class EmptyProcessorSupplier implements ProcessorSupplier<Object, Object, Void, Void> {
    @Override
    public Processor<Object, Object, Void, Void> get() {
        return new EmptyProcessor();
    }
}
