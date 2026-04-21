package com.acea.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value
public class AggregationKey {
    String adId;
    long minute;
    int filterId;

    @JsonCreator
    public AggregationKey(
            @JsonProperty("adId") String adId,
            @JsonProperty("minute") long minute,
            @JsonProperty("filterId") int filterId) {
        this.adId = adId;
        this.minute = minute;
        this.filterId = filterId;
    }

    public String asStringKey() {
        return adId + "|" + minute + "|" + filterId;
    }
}
