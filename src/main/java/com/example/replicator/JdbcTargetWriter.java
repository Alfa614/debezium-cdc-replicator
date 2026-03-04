package com.example.replicator;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;

@Slf4j
@Component
public class JdbcTargetWriter {

    private final Connection connection;

    public JdbcTargetWriter(
            @Value("${target.jdbc.url}")      String url,
            @Value("${target.jdbc.user}")     String user,
            @Value("${target.jdbc.password}") String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, user, password);
        log.info("✅ Connected to target PostgreSQL successfully");
    }

    public void upsert(String table, JsonNode row) {
        if (row == null || row.isEmpty()) return;

        List<String> columns = new ArrayList<>();
        List<Object> values  = new ArrayList<>();

        row.fields().forEachRemaining(entry -> {
            columns.add("\"" + entry.getKey() + "\"");
            values.add(nodeToValue(entry.getValue()));
        });

        String columnList    = String.join(", ", columns);
        String placeholders  = String.join(", ", Collections.nCopies(columns.size(), "?"));
        String firstCol      = columns.get(0);
        String updateSet     = String.join(", ",
                columns.stream().map(c -> c + " = EXCLUDED." + c).toList());

        String sql = String.format(
                "INSERT INTO \"%s\" (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                table, columnList, placeholders, firstCol, updateSet);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) stmt.setObject(i + 1, values.get(i));
            stmt.executeUpdate();
            log.info("Upserted row into {}", table);
        } catch (SQLException e) {
            log.error("Upsert failed for {}: {}", table, e.getMessage());
        }
    }

    public void delete(String table, JsonNode row) {
        if (row == null || row.isEmpty()) return;

        Map.Entry<String, JsonNode> pk = row.fields().next();
        String sql = String.format("DELETE FROM \"%s\" WHERE \"%s\" = ?", table, pk.getKey());

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, nodeToValue(pk.getValue()));
            stmt.executeUpdate();
            log.info("Deleted row from {}", table);
        } catch (SQLException e) {
            log.error("Delete failed for {}: {}", table, e.getMessage());
        }
    }

    private Object nodeToValue(JsonNode node) {
        if (node.isNull())    return null;
        if (node.isBoolean()) return node.booleanValue();
        if (node.isInt())     return node.intValue();
        if (node.isLong()) {
            long val = node.longValue();
            if (val > 1_000_000_000_000_000L) {
                return new java.sql.Timestamp(val / 1_000_000);
            }
            return val;
        }
        if (node.isDouble())  return node.doubleValue();
        if (node.isBinary() || (node.isTextual() && isBase64Bytes(node.asText()))) {
            try {
                byte[] bytes = java.util.Base64.getDecoder().decode(node.asText());
                return new java.math.BigDecimal(new java.math.BigInteger(bytes), 2); // scale=2 for DECIMAL(10,2)
            } catch (Exception e) {
            }
        }
        if (node.isTextual()) {
            try { return new java.math.BigDecimal(node.asText()); }
            catch (NumberFormatException e) { return node.asText(); }
        }
        return node.asText();
    }

    private boolean isBase64Bytes(String s) {
        return s.matches("^[A-Za-z0-9+/]*={0,2}$") && s.length() % 4 == 0 && s.length() <= 24;
    }
}