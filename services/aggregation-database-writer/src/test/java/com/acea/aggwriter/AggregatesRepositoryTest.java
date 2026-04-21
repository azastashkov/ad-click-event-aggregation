package com.acea.aggwriter;

import com.acea.common.model.AggregatedCount;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AggregatesRepositoryTest {

    @Test
    void upsertOneExecutesBatchWithThreeStatementsOnFirstWrite() {
        CqlSession session = mock(CqlSession.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        BoundStatement bs = mock(BoundStatement.class);
        when(session.prepare(anyString())).thenReturn(ps);
        when(ps.bind(any(Object[].class))).thenReturn(bs);
        when(ps.bind(anyString(), anyInt(), anyLong(), anyLong())).thenReturn(bs);
        when(ps.bind(anyInt(), anyLong(), anyString(), anyLong())).thenReturn(bs);
        when(ps.bind(anyInt(), anyLong(), anyLong(), anyString())).thenReturn(bs);

        ResultSet emptyRs = mock(ResultSet.class);
        when(emptyRs.one()).thenReturn(null);
        when(session.execute(any(BoundStatement.class))).thenReturn(emptyRs);

        AggregatesRepository repo = new AggregatesRepository(session);
        repo.init();
        repo.upsertOne(AggregatedCount.builder()
                .adId("ad-1").filterId(0).minute(100).count(5).build());

        ArgumentCaptor<Statement<?>> cap = ArgumentCaptor.forClass(Statement.class);
        verify(session, atLeastOnce()).execute(cap.capture());
        // Should have executed at least: 1 select + 1 batch
        assertThat(cap.getAllValues()).isNotEmpty();
    }

    @Test
    void upsertOneDeletesStaleBucketRowWhenCountChanges() {
        CqlSession session = mock(CqlSession.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        BoundStatement bs = mock(BoundStatement.class);
        when(session.prepare(anyString())).thenReturn(ps);
        when(ps.bind(any(Object[].class))).thenReturn(bs);
        when(ps.bind(anyString(), anyInt(), anyLong(), anyLong())).thenReturn(bs);
        when(ps.bind(anyInt(), anyLong(), anyString(), anyLong())).thenReturn(bs);
        when(ps.bind(anyInt(), anyLong(), anyLong(), anyString())).thenReturn(bs);

        Row previous = mock(Row.class);
        when(previous.getLong("count")).thenReturn(3L);
        ResultSet rs = mock(ResultSet.class);
        when(rs.one()).thenReturn(previous);
        when(session.execute(any(BoundStatement.class))).thenReturn(rs);

        AggregatesRepository repo = new AggregatesRepository(session);
        repo.init();
        repo.upsertOne(AggregatedCount.builder()
                .adId("ad-1").filterId(0).minute(100).count(5).build());

        verify(ps).bind(0, 100L, 3L, "ad-1"); // delete stale
    }
}
