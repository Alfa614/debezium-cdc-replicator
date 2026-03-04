package com.example.replicator;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
public class DebeziumEngineConfig {

    @Value("${debezium.database.hostname}")       private String hostname;
    @Value("${debezium.database.port}")           private String port;
    @Value("${debezium.database.user}")           private String dbUser;
    @Value("${debezium.database.password}")       private String dbPassword;
    @Value("${debezium.database.dbname}")         private String dbName;
    @Value("${debezium.database.encrypt}")        private String encrypt;
    @Value("${debezium.topic.prefix}")            private String topicPrefix;
    @Value("${debezium.offset.storage.jdbc.url}") private String offsetUrl;
    @Value("${debezium.offset.storage.jdbc.user}")private String offsetUser;
    @Value("${debezium.offset.storage.jdbc.password}") private String offsetPassword;

    private final DebeziumEventHandler eventHandler;
    private DebeziumEngine<ChangeEvent<String, String>> engine;

    @PostConstruct
    public void start() {
        Properties props = new Properties();

        // Engine
        props.setProperty("name", "mssql-to-postgres");
        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");

        // Source - SQL Server
        props.setProperty("database.hostname", hostname);
        props.setProperty("database.port", port);
        props.setProperty("database.user", dbUser);
        props.setProperty("database.password", dbPassword);
        props.setProperty("database.names", dbName);
        props.setProperty("database.encrypt", encrypt);
        props.setProperty("topic.prefix", topicPrefix);
        props.setProperty("table.include.list", "dbo.*");

        // Offset storage in PostgreSQL
        props.setProperty("offset.storage",
                "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
        props.setProperty("offset.storage.jdbc.url", offsetUrl);
        props.setProperty("offset.storage.jdbc.user", offsetUser);
        props.setProperty("offset.storage.jdbc.password", offsetPassword);
        props.setProperty("offset.storage.jdbc.table.name", "debezium_offsets");
        props.setProperty("offset.flush.interval.ms", "5000");

        // Schema history in PostgreSQL
        props.setProperty("schema.history.internal",
                "io.debezium.storage.jdbc.history.JdbcSchemaHistory");
        props.setProperty("schema.history.internal.jdbc.url", offsetUrl);
        props.setProperty("schema.history.internal.jdbc.user", offsetUser);
        props.setProperty("schema.history.internal.jdbc.password", offsetPassword);
        props.setProperty("schema.history.internal.jdbc.table.name", "debezium_schema_history");

        // Build and run engine
        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(eventHandler::handleEvent)
                .build();

        Executors.newSingleThreadExecutor().execute(engine);
        System.out.println("Debezium engine started!");
    }

    @PreDestroy
    public void stop() throws IOException {
        if (engine != null) engine.close();
    }
}