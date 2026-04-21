package com.acea.query;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryControllerTest {

    @Test
    void aggregatedCountDefaultsFromTo() {
        QueryService svc = mock(QueryService.class);
        when(svc.aggregatedCount(anyString(), anyInt(), anyLong(), anyLong())).thenReturn(7L);
        QueryController c = new QueryController(svc);
        Map<String, Object> body = c.aggregatedCount("ad-9", null, null, 0);
        assertThat(body).containsEntry("ad_id", "ad-9").containsEntry("count", 7L);
    }

    @Test
    void popularAdsUsesDefaults() {
        QueryService svc = mock(QueryService.class);
        when(svc.popularAds(anyInt(), anyInt(), anyInt())).thenReturn(List.of("ad-1", "ad-2"));
        QueryController c = new QueryController(svc);
        Map<String, Object> body = c.popularAds(10, 5, 0);
        assertThat(body.get("ad_ids")).isEqualTo(List.of("ad-1", "ad-2"));
    }
}
