package dev.knoepfle.Processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueMapper;

public class CoolerMessageMapper implements ValueMapper<JsonNode, JsonNode> {
    @Override
    public JsonNode apply(JsonNode jsonNode) {
        ObjectNode value = (ObjectNode) jsonNode;
        if (value.get("temperature").isNumber()) {
            if(value.get("temperature").asDouble() < 0 || value.get("temperature").asDouble() > 20) {
                value.putNull("temperature");
            }
        }
        if (value.get("doorOpenTime").isNumber()) {
            if(value.get("doorOpenTime").asInt() > 100000) {
                value.putNull("doorOpenTime");
            }
        }
        value.put("deduplicationHash",
                31 * value.get("cng_deviceId").asText().hashCode() + value.get("timestamp").asText().hashCode());
        return value;
    }
}
