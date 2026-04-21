package com.acea.recalc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@RestController
@RequiredArgsConstructor
public class RecalcController {

    private final RecalcService service;
    private final Executor executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "recalc-worker");
        t.setDaemon(true);
        return t;
    });

    @PostMapping("/recalc")
    public ResponseEntity<Map<String, Object>> recalc(
            @RequestParam(name = "from", required = false) Long from,
            @RequestParam(name = "to", required = false) Long to,
            @RequestParam(name = "pageSize", required = false, defaultValue = "500") int pageSize) {
        long now = Instant.now().getEpochSecond();
        long fromEpoch = from == null ? now - 3600 : from;
        long toEpoch = to == null ? now : to;

        if (service.isRunning()) {
            return ResponseEntity.status(409).body(Map.of("error", "already-running"));
        }
        CompletableFuture.runAsync(() -> {
            try {
                service.runBlocking(fromEpoch, toEpoch, pageSize);
            } catch (Exception e) {
                log.error("recalc failed", e);
            }
        }, executor);
        return ResponseEntity.accepted().body(Map.of(
                "status", "started",
                "from", fromEpoch,
                "to", toEpoch));
    }

    @PostMapping("/recalc/status")
    public Map<String, Object> status() {
        return Map.of("running", service.isRunning());
    }
}
