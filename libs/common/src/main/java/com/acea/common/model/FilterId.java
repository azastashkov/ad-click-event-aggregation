package com.acea.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum FilterId {
    NONE(0),
    US_ONLY(1),
    MOBILE_ONLY(2);

    private final int id;

    FilterId(int id) {
        this.id = id;
    }

    @JsonValue
    public int id() {
        return id;
    }

    @JsonCreator
    public static FilterId fromId(int id) {
        for (FilterId f : values()) {
            if (f.id == id) {
                return f;
            }
        }
        throw new IllegalArgumentException("Unknown filter id: " + id);
    }
}
