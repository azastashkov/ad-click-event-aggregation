package com.acea.recalc;

import com.acea.common.Topics;
import com.acea.common.model.ClickEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecalcService {

    private final RawClickReader reader;
    private final KafkaTemplate<String, ClickEvent> kafka;
    private final MeterRegistry meters;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Counter published;
    private Counter errors;

    public boolean isRunning() {
        return running.get();
    }

    public long runBlocking(long fromEpochSec, long toEpochSec, int pageSize) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("recalc already running");
        }
        if (published == null) {
            published = Counter.builder("recalc_published_total").register(meters);
            errors = Counter.builder("recalc_errors_total").register(meters);
        }
        long sent = 0;
        try {
            Iterator<ClickEvent> iter = reader.scanRange(fromEpochSec, toEpochSec, pageSize);
            while (iter.hasNext()) {
                ClickEvent e = iter.next();
                try {
                    kafka.send(Topics.RECALC_AD_CLICK_EVENTS, e.getAdId(), e);
                    published.increment();
                    sent++;
                    if (sent % 1000 == 0) {
                        kafka.flush();
                        log.info("recalc progress sent={}", sent);
                    }
                } catch (Exception ex) {
                    errors.increment();
                    log.warn("recalc publish failed ad={} err={}", e.getAdId(), ex.getMessage());
                }
            }
            kafka.flush();
        } finally {
            running.set(false);
        }
        log.info("recalc completed sent={} from={} to={}", sent, fromEpochSec, toEpochSec);
        return sent;
    }
}
