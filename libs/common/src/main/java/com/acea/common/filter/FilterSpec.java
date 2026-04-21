package com.acea.common.filter;

import com.acea.common.model.ClickEvent;
import com.acea.common.model.FilterId;

import java.util.ArrayList;
import java.util.List;

public final class FilterSpec {

    private FilterSpec() {}

    public static List<FilterId> applicableFilters(ClickEvent event) {
        List<FilterId> applicable = new ArrayList<>(3);
        applicable.add(FilterId.NONE);
        if ("US".equalsIgnoreCase(event.getCountryCode())) {
            applicable.add(FilterId.US_ONLY);
        }
        if ("MOBILE".equalsIgnoreCase(event.getDeviceType())) {
            applicable.add(FilterId.MOBILE_ONLY);
        }
        return applicable;
    }
}
