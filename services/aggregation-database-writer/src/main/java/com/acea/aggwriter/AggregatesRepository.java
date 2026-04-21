package com.acea.aggwriter;

import com.acea.common.model.AggregatedCount;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class AggregatesRepository {

    private static final String UPSERT_BY_AD = """
            INSERT INTO aggregates.by_ad (ad_id, filter_id, minute, count)
              VALUES (?, ?, ?, ?)
            """;
    private static final String UPSERT_LAST = """
            INSERT INTO aggregates.last_count (filter_id, minute, ad_id, count)
              VALUES (?, ?, ?, ?)
            """;
    private static final String UPSERT_BY_BUCKET = """
            INSERT INTO aggregates.by_bucket (filter_id, minute, count, ad_id)
              VALUES (?, ?, ?, ?)
            """;
    private static final String SELECT_LAST = """
            SELECT count FROM aggregates.last_count
              WHERE filter_id=? AND minute=? AND ad_id=?
            """;
    private static final String DELETE_BY_BUCKET = """
            DELETE FROM aggregates.by_bucket
              WHERE filter_id=? AND minute=? AND count=? AND ad_id=?
            """;

    private final CqlSession session;
    private PreparedStatement byAdStmt;
    private PreparedStatement lastStmt;
    private PreparedStatement byBucketStmt;
    private PreparedStatement selectLast;
    private PreparedStatement deleteByBucket;

    @PostConstruct
    void init() {
        byAdStmt = session.prepare(UPSERT_BY_AD);
        lastStmt = session.prepare(UPSERT_LAST);
        byBucketStmt = session.prepare(UPSERT_BY_BUCKET);
        selectLast = session.prepare(SELECT_LAST);
        deleteByBucket = session.prepare(DELETE_BY_BUCKET);
    }

    public void upsertAll(List<AggregatedCount> counts) {
        if (counts.isEmpty()) {
            return;
        }
        for (AggregatedCount c : counts) {
            upsertOne(c);
        }
    }

    void upsertOne(AggregatedCount c) {
        Long previous = fetchPreviousCount(c);
        BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);
        if (previous != null && previous != c.getCount()) {
            batch.addStatement(deleteByBucket.bind(c.getFilterId(), c.getMinute(), previous, c.getAdId()));
        }
        batch.addStatement(byAdStmt.bind(c.getAdId(), c.getFilterId(), c.getMinute(), c.getCount()));
        batch.addStatement(lastStmt.bind(c.getFilterId(), c.getMinute(), c.getAdId(), c.getCount()));
        batch.addStatement(byBucketStmt.bind(c.getFilterId(), c.getMinute(), c.getCount(), c.getAdId()));
        session.execute(batch.build());
    }

    private Long fetchPreviousCount(AggregatedCount c) {
        Row r = session.execute(selectLast.bind(c.getFilterId(), c.getMinute(), c.getAdId())).one();
        return r == null ? null : r.getLong("count");
    }
}
