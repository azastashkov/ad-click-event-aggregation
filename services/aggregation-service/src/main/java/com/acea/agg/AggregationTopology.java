package com.acea.agg;

import org.apache.kafka.streams.Topology;

public final class AggregationTopology {

    private AggregationTopology() {}

    public static Topology build(String sourceTopic, String sinkTopic) {
        return com.acea.common.streams.AggregationTopology.build(sourceTopic, sinkTopic, "agg-store");
    }
}
