package com.acea.query;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryServiceTest {

    @Test
    void aggregatedCountDelegatesToRepo() {
        AggregateQueryRepository repo = mock(AggregateQueryRepository.class);
        when(repo.sumForAd("ad-1", 0, 100, 105)).thenReturn(42L);
        QueryService s = new QueryService(repo);
        assertThat(s.aggregatedCount("ad-1", 0, 100, 105)).isEqualTo(42L);
    }

    @Test
    void popularAdsReturnsAdIdsFromRepo() {
        AggregateQueryRepository repo = mock(AggregateQueryRepository.class);
        when(repo.topForWindow(anyInt(), anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(
                        new AggregateQueryRepository.AdCount("ad-1", 10),
                        new AggregateQueryRepository.AdCount("ad-2", 5)));
        QueryService s = new QueryService(repo);
        assertThat(s.popularAds(5, 5, 0)).containsExactly("ad-1", "ad-2");
    }
}
