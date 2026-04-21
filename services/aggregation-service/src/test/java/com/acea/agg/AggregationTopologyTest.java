package com.acea.agg;

import com.acea.common.kafka.JsonSerde;
import com.acea.common.model.AggregatedCount;
import com.acea.common.model.ClickEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class AggregationTopologyTest {

    private static final String SOURCE = "ad-click-events";
    private static final String SINK = "aggregated-counts";

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, AggregatedCount> output;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        Topology topology = AggregationTopology.build(SOURCE, SINK);
        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(SOURCE, new StringSerializer(),
                JsonSerde.of(ClickEvent.class).serializer());
        output = driver.createOutputTopic(SINK, new StringDeserializer(),
                JsonSerde.of(AggregatedCount.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) driver.close();
    }

    @Test
    void usMobileEventEmitsThreeFilterAggregates() {
        long ts = Instant.parse("2026-04-20T10:00:15Z").toEpochMilli();
        input.pipeInput("ad-1", ClickEvent.builder()
                .eventId("e1").adId("ad-1").userId("u1")
                .countryCode("US").deviceType("MOBILE").clickTs(ts).build());
        driver.advanceWallClockTime(Duration.ofMinutes(2));
        input.pipeInput("ad-1", ClickEvent.builder()
                .eventId("e2").adId("ad-1").userId("u2")
                .countryCode("DE").deviceType("DESKTOP")
                .clickTs(ts + Duration.ofMinutes(2).toMillis()).build());
        driver.advanceWallClockTime(Duration.ofMinutes(2));

        List<AggregatedCount> records = output.readValuesToList();
        Map<Integer, Long> sumByFilter = new HashMap<>();
        for (AggregatedCount c : records) {
            sumByFilter.merge(c.getFilterId(), c.getCount(), Math::max);
        }
        // NONE filter accumulates both events.
        assertThat(sumByFilter.get(0)).isEqualTo(1L);
        // US-only accumulates only the first event.
        assertThat(sumByFilter.get(1)).isEqualTo(1L);
        // MOBILE-only accumulates only the first event.
        assertThat(sumByFilter.get(2)).isEqualTo(1L);
    }

    @Test
    void sameAdSameMinuteFilterSumsCounts() {
        long ts = Instant.parse("2026-04-20T10:00:15Z").toEpochMilli();
        for (int i = 0; i < 5; i++) {
            input.pipeInput("ad-1", ClickEvent.builder()
                    .eventId("e" + i).adId("ad-1").userId("u" + i)
                    .countryCode("US").deviceType("DESKTOP").clickTs(ts + i * 100).build());
        }
        driver.advanceWallClockTime(Duration.ofMinutes(2));

        List<AggregatedCount> records = output.readValuesToList();
        // Find the last NONE filter record — it should show 5.
        long noneCount = 0;
        for (AggregatedCount c : records) {
            if (c.getFilterId() == 0) noneCount = c.getCount();
        }
        assertThat(noneCount).isEqualTo(5L);
    }

    @Test
    void nullAdIdDroppedCleanly() {
        long ts = Instant.parse("2026-04-20T10:00:15Z").toEpochMilli();
        input.pipeInput("ad-?", ClickEvent.builder()
                .eventId("e1").adId(null).userId("u1")
                .countryCode("US").deviceType("MOBILE").clickTs(ts).build());
        driver.advanceWallClockTime(Duration.ofMinutes(2));
        assertThat(output.isEmpty()).isTrue();
    }
}
