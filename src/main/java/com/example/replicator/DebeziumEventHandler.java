package com.example.replicator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DebeziumEventHandler {

    private final JdbcTargetWriter writer;
    private final ObjectMapper mapper = new ObjectMapper();

    public void handleEvent(ChangeEvent<String, String> record) {
        try {
            String value = record.value();
            if (value == null) return;

            JsonNode root     = mapper.readTree(value);
            JsonNode payload  = root.path("payload");
            String operation  = payload.path("op").asText();

            // Topic format: prefix.schema.tablename
            String topic      = record.destination();
            String tableName  = topic.contains(".")
                    ? topic.substring(topic.lastIndexOf('.') + 1)
                    : topic;

            JsonNode after  = payload.path("after");
            JsonNode before = payload.path("before");

            log.info("CDC Event → op={} table={}", operation, tableName);

            switch (operation) {
                case "c", "r" -> writer.upsert(tableName, after);
                case "u"      -> writer.upsert(tableName, after);
                case "d"      -> writer.delete(tableName, before);
                default       -> log.warn("Unknown operation: {}", operation);
            }

        } catch (Exception e) {
            log.error("Error processing CDC event: {}", e.getMessage(), e);
        }
    }
}