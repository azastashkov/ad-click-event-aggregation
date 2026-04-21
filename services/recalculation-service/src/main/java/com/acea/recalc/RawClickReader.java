package com.acea.recalc;

import com.acea.common.model.ClickEvent;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.NoSuchElementException;

@Component
@RequiredArgsConstructor
public class RawClickReader {

    private static final String TOKEN_SCAN = """
            SELECT ad_id, bucket_hour, click_ts, event_id, user_id, country_code, device_type
              FROM raw.ad_clicks
              WHERE token(ad_id, bucket_hour) >= token(?, ?)
              LIMIT ?
              ALLOW FILTERING
            """;

    private final CqlSession session;

    public Iterator<ClickEvent> scanRange(long fromEpochSec, long toEpochSec, int pageSize) {
        SimpleStatement stmt = SimpleStatement.newInstance(
                        "SELECT ad_id, bucket_hour, click_ts, event_id, user_id, country_code, device_type " +
                                "FROM raw.ad_clicks")
                .setPageSize(pageSize);
        Iterator<Row> rawIter = session.execute(stmt).iterator();
        return new Iterator<>() {
            private ClickEvent next;

            @Override
            public boolean hasNext() {
                while (next == null && rawIter.hasNext()) {
                    Row r = rawIter.next();
                    long clickTs = r.getInstant("click_ts") == null ? 0 : r.getInstant("click_ts").toEpochMilli();
                    long sec = clickTs / 1000;
                    if (sec < fromEpochSec || sec > toEpochSec) {
                        continue;
                    }
                    next = ClickEvent.builder()
                            .eventId(String.valueOf(r.getUuid("event_id")))
                            .adId(r.getString("ad_id"))
                            .userId(r.getString("user_id"))
                            .countryCode(r.getString("country_code"))
                            .deviceType(r.getString("device_type"))
                            .clickTs(clickTs)
                            .build();
                }
                return next != null;
            }

            @Override
            public ClickEvent next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ClickEvent out = next;
                next = null;
                return out;
            }
        };
    }
}
