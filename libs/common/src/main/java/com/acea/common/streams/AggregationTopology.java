package com.acea.common.streams;

import com.acea.common.filter.FilterSpec;
import com.acea.common.kafka.JsonSerde;
import com.acea.common.model.AggregatedCount;
import com.acea.common.model.AggregationKey;
import com.acea.common.model.ClickEvent;
import com.acea.common.model.FilterId;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public final class AggregationTopology {

    private AggregationTopology() {}

    public static Topology build(String sourceTopic, String sinkTopic, String storeName) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<ClickEvent> clickSerde = JsonSerde.of(ClickEvent.class);
        Serde<Long> longSerde = Serdes.Long();
        Serde<AggregatedCount> aggSerde = JsonSerde.of(AggregatedCount.class);

        KStream<String, ClickEvent> source = builder.stream(sourceTopic,
                Consumed.with(stringSerde, clickSerde));

        KStream<String, Long> mapped = source.flatMap((k, v) -> {
            if (v == null || v.getAdId() == null) {
                return List.of();
            }
            long minute = Instant.ofEpochMilli(v.getClickTs())
                    .truncatedTo(ChronoUnit.MINUTES)
                    .getEpochSecond() / 60;
            List<KeyValue<String, Long>> out = new ArrayList<>(3);
            for (FilterId f : FilterSpec.applicableFilters(v)) {
                AggregationKey key = new AggregationKey(v.getAdId(), minute, f.id());
                out.add(KeyValue.pair(key.asStringKey(), 1L));
            }
            return out;
        });

        KTable<Windowed<String>, Long> counts = mapped
                .groupByKey(Grouped.with(stringSerde, longSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(5)))
                .aggregate(
                        () -> 0L,
                        (k, v, agg) -> agg + v,
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(stringSerde)
                                .withValueSerde(longSerde));

        counts.toStream()
                .map((wk, v) -> {
                    AggregationKey ak = parse(wk.key());
                    return KeyValue.pair(ak.getAdId(),
                            AggregatedCount.builder()
                                    .adId(ak.getAdId())
                                    .minute(ak.getMinute())
                                    .filterId(ak.getFilterId())
                                    .count(v)
                                    .build());
                })
                .to(sinkTopic, Produced.with(stringSerde, aggSerde));

        return builder.build();
    }

    private static AggregationKey parse(String stringKey) {
        String[] parts = stringKey.split("\\|");
        return new AggregationKey(parts[0], Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    }
}
