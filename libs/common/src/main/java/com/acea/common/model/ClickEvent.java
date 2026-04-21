package com.acea.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class ClickEvent {
    String eventId;
    String adId;
    String userId;
    String countryCode;
    String deviceType;
    long clickTs;

    @JsonCreator
    public ClickEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("adId") String adId,
            @JsonProperty("userId") String userId,
            @JsonProperty("countryCode") String countryCode,
            @JsonProperty("deviceType") String deviceType,
            @JsonProperty("clickTs") long clickTs) {
        this.eventId = eventId;
        this.adId = adId;
        this.userId = userId;
        this.countryCode = countryCode;
        this.deviceType = deviceType;
        this.clickTs = clickTs;
    }
}
