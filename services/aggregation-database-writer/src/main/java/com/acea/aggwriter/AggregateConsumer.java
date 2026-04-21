package com.acea.aggwriter;

import com.acea.common.Topics;
import com.acea.common.model.AggregatedCount;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregateConsumer {

    private final AggregatesRepository repo;
    private final MeterRegistry meters;

    private Counter realtimeCounter;
    private Counter recalcCounter;
    private Counter errorCounter;

    @KafkaListener(
            topics = Topics.AGGREGATED_COUNTS,
            groupId = "agg-writer-realtime",
            containerFactory = "aggBatchContainer"
    )
    public void onRealtime(List<AggregatedCount> batch) {
        persist(batch, "realtime");
    }

    @KafkaListener(
            topics = Topics.RECALC_AGGREGATED_COUNTS,
            groupId = "agg-writer-recalc",
            containerFactory = "aggBatchContainer"
    )
    public void onRecalc(List<AggregatedCount> batch) {
        persist(batch, "recalc");
    }

    private void persist(List<AggregatedCount> batch, String source) {
        if (realtimeCounter == null) {
            realtimeCounter = Counter.builder("agg_writer_persisted_total").tag("source", "realtime").register(meters);
            recalcCounter = Counter.builder("agg_writer_persisted_total").tag("source", "recalc").register(meters);
            errorCounter = Counter.builder("agg_writer_errors_total").register(meters);
        }
        try {
            repo.upsertAll(batch);
            ("realtime".equals(source) ? realtimeCounter : recalcCounter).increment(batch.size());
        } catch (Exception e) {
            errorCounter.increment();
            log.warn("agg-writer batch failed size={} source={} err={}",
                    batch.size(), source, e.getMessage());
            throw e;
        }
    }
}
