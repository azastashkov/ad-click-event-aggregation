package com.acea.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class AggregatedCount {
    String adId;
    int filterId;
    long minute;
    long count;

    @JsonCreator
    public AggregatedCount(
            @JsonProperty("adId") String adId,
            @JsonProperty("filterId") int filterId,
            @JsonProperty("minute") long minute,
            @JsonProperty("count") long count) {
        this.adId = adId;
        this.filterId = filterId;
        this.minute = minute;
        this.count = count;
    }
}
