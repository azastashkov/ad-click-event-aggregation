package com.acea.query;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class AggregateQueryRepository {

    private static final String SELECT_BY_AD = """
            SELECT count FROM aggregates.by_ad
              WHERE ad_id=? AND filter_id=? AND minute >= ? AND minute <= ?
            """;
    private static final String SELECT_BY_BUCKET = """
            SELECT ad_id, count FROM aggregates.by_bucket
              WHERE filter_id=? AND minute=?
            """;

    private final CqlSession session;
    private PreparedStatement selectByAd;
    private PreparedStatement selectByBucket;

    @PostConstruct
    void init() {
        selectByAd = session.prepare(SELECT_BY_AD);
        selectByBucket = session.prepare(SELECT_BY_BUCKET);
    }

    public long sumForAd(String adId, int filterId, long fromMinute, long toMinute) {
        long sum = 0;
        for (Row r : session.execute(selectByAd.bind(adId, filterId, fromMinute, toMinute))) {
            sum += r.getLong("count");
        }
        return sum;
    }

    public List<AdCount> topForWindow(int filterId, long fromMinute, long toMinute, int limit) {
        Map<String, Long> aggregate = new HashMap<>();
        for (long minute = fromMinute; minute <= toMinute; minute++) {
            for (Row r : session.execute(selectByBucket.bind(filterId, minute))) {
                String adId = r.getString("ad_id");
                long c = r.getLong("count");
                aggregate.merge(adId, c, Long::sum);
            }
        }
        List<AdCount> out = new ArrayList<>(aggregate.size());
        aggregate.forEach((ad, c) -> out.add(new AdCount(ad, c)));
        out.sort((a, b) -> Long.compare(b.count, a.count));
        return out.size() <= limit ? out : out.subList(0, limit);
    }

    @Value
    public static class AdCount {
        String adId;
        long count;
    }
}
