package com.acea.rawwriter;

import com.acea.common.Topics;
import com.acea.common.model.ClickEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClickConsumer {

    private final RawClickRepository repo;
    private final MeterRegistry meters;

    private Counter persisted;
    private Counter errors;
    private Timer persistTimer;

    @KafkaListener(
            topics = Topics.AD_CLICK_EVENTS,
            groupId = "raw-writer",
            containerFactory = "batchListenerContainerFactory"
    )
    public void onBatch(List<ClickEvent> events) {
        if (persistTimer == null) {
            persistTimer = Timer.builder("raw_writer_persist_seconds").register(meters);
            persisted = Counter.builder("raw_writer_persisted_total").register(meters);
            errors = Counter.builder("raw_writer_errors_total").register(meters);
        }
        persistTimer.record(() -> {
            try {
                repo.saveAll(events);
                persisted.increment(events.size());
            } catch (Exception e) {
                errors.increment();
                log.warn("Cassandra batch failed size={} err={}", events.size(), e.getMessage());
                throw e;
            }
        });
    }
}
