package com.acea.rawwriter;

import com.acea.common.model.ClickEvent;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class RawClickRepository {

    private static final String INSERT_CQL = """
            INSERT INTO raw.ad_clicks
              (ad_id, bucket_hour, click_ts, event_id, user_id, country_code, device_type)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final CqlSession session;
    private PreparedStatement insert;

    @PostConstruct
    void prepareStatements() {
        this.insert = session.prepare(INSERT_CQL);
    }

    public void saveAll(List<ClickEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);
        for (ClickEvent e : events) {
            batch.addStatement(insert.bind(
                    e.getAdId(),
                    bucketHour(e.getClickTs()),
                    Instant.ofEpochMilli(e.getClickTs()),
                    parseUuid(e.getEventId()),
                    e.getUserId(),
                    e.getCountryCode(),
                    e.getDeviceType()));
        }
        session.execute(batch.build());
    }

    static long bucketHour(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis)
                .truncatedTo(ChronoUnit.HOURS).getEpochSecond() / 3600;
    }

    static UUID parseUuid(String s) {
        try {
            return UUID.fromString(s);
        } catch (Exception e) {
            return UUID.randomUUID();
        }
    }
}
