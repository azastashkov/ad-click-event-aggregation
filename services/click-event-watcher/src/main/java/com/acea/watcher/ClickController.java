package com.acea.watcher;

import com.acea.common.Topics;
import com.acea.common.model.ClickEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
public class ClickController {

    private final KafkaTemplate<String, ClickEvent> kafka;
    private final MeterRegistry meters;

    private Counter accepted;
    private Counter failed;

    private Counter accepted() {
        if (accepted == null) {
            accepted = Counter.builder("watcher.clicks.accepted").register(meters);
        }
        return accepted;
    }

    private Counter failed() {
        if (failed == null) {
            failed = Counter.builder("watcher.clicks.failed").register(meters);
        }
        return failed;
    }

    @PostMapping(value = "/clicks", consumes = "application/json")
    public ResponseEntity<Map<String, String>> postClick(@RequestBody ClickEvent event) {
        return publish(normalize(event));
    }

    @GetMapping(value = "/clicks")
    public ResponseEntity<Map<String, String>> getClick(
            @RequestParam(name = "adId") String adId,
            @RequestParam(name = "userId", required = false) String userId,
            @RequestParam(name = "countryCode", required = false, defaultValue = "US") String countryCode,
            @RequestParam(name = "deviceType", required = false, defaultValue = "DESKTOP") String deviceType,
            @RequestParam(name = "clickTs", required = false, defaultValue = "0") long clickTs) {
        ClickEvent evt = ClickEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .adId(adId)
                .userId(userId == null ? "anon" : userId)
                .countryCode(countryCode)
                .deviceType(deviceType)
                .clickTs(clickTs == 0 ? Instant.now().toEpochMilli() : clickTs)
                .build();
        return publish(evt);
    }

    private ClickEvent normalize(ClickEvent in) {
        return in.toBuilder()
                .eventId(in.getEventId() == null || in.getEventId().isBlank()
                        ? UUID.randomUUID().toString() : in.getEventId())
                .clickTs(in.getClickTs() == 0 ? Instant.now().toEpochMilli() : in.getClickTs())
                .build();
    }

    private ResponseEntity<Map<String, String>> publish(ClickEvent event) {
        if (event.getAdId() == null || event.getAdId().isBlank()) {
            failed().increment();
            return ResponseEntity.badRequest().body(Map.of("error", "adId required"));
        }
        try {
            kafka.send(Topics.AD_CLICK_EVENTS, event.getAdId(), event).get();
            accepted().increment();
            return ResponseEntity.accepted().body(Map.of("eventId", event.getEventId()));
        } catch (Exception e) {
            failed().increment();
            log.warn("Kafka publish failed adId={} err={}", event.getAdId(), e.getMessage());
            return ResponseEntity.status(503).body(Map.of("error", "publish-failed"));
        }
    }
}
