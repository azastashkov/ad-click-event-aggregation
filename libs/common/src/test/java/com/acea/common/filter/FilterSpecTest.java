package com.acea.common.filter;

import com.acea.common.model.ClickEvent;
import com.acea.common.model.FilterId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FilterSpecTest {

    @Test
    void usMobileEventMatchesAllFilters() {
        ClickEvent e = ClickEvent.builder()
                .eventId("e1").adId("a1").userId("u1")
                .countryCode("US").deviceType("MOBILE").clickTs(0L).build();
        assertThat(FilterSpec.applicableFilters(e))
                .containsExactlyInAnyOrder(FilterId.NONE, FilterId.US_ONLY, FilterId.MOBILE_ONLY);
    }

    @Test
    void nonUsDesktopOnlyMatchesNone() {
        ClickEvent e = ClickEvent.builder()
                .eventId("e1").adId("a1").userId("u1")
                .countryCode("DE").deviceType("DESKTOP").clickTs(0L).build();
        assertThat(FilterSpec.applicableFilters(e)).containsExactly(FilterId.NONE);
    }
}
