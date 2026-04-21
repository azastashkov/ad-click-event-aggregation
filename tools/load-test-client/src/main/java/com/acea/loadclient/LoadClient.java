package com.acea.loadclient;

import com.acea.common.kafka.JsonSerde;
import com.acea.common.model.ClickEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class LoadClient {

    private static final ObjectMapper MAPPER = JsonSerde.mapper();
    private static final Random RAND = new Random(42);
    private static final String[] COUNTRIES = {"US", "US", "US", "DE", "FR", "JP", "BR"};
    private static final String[] DEVICES = {"MOBILE", "MOBILE", "DESKTOP", "TABLET"};

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong err = new AtomicLong();

    private final String targetUrl;
    private final String recalcUrl;
    private final String queryUrl;
    private final int rps;
    private final int warmupSec;
    private final int durationSec;
    private final int cooldownSec;
    private final int adCardinality;
    private final int metricsPort;

    public LoadClient(Config c) {
        this.targetUrl = c.targetUrl;
        this.recalcUrl = c.recalcUrl;
        this.queryUrl = c.queryUrl;
        this.rps = c.rps;
        this.warmupSec = c.warmupSec;
        this.durationSec = c.durationSec;
        this.cooldownSec = c.cooldownSec;
        this.adCardinality = c.adCardinality;
        this.metricsPort = c.metricsPort;
    }

    public static void main(String[] args) throws Exception {
        Config c = Config.fromEnv();
        System.out.println("Starting load client: " + c);
        LoadClient client = new LoadClient(c);
        client.run();
    }

    public void run() throws Exception {
        startMetricsServer();
        waitForHealth(targetUrl);

        long runStart = Instant.now().getEpochSecond();
        System.out.println("Phase 1: warmup " + warmupSec + "s @ " + (rps / 4) + " rps");
        runPhase(warmupSec, Math.max(1, rps / 4));

        System.out.println("Phase 2: sustained " + durationSec + "s @ " + rps + " rps");
        ScheduledExecutorService recalcTrigger = Executors.newSingleThreadScheduledExecutor();
        recalcTrigger.schedule(() -> triggerRecalc(runStart), durationSec / 2, TimeUnit.SECONDS);
        runPhase(durationSec, rps);
        recalcTrigger.shutdown();

        System.out.println("Phase 3: cooldown " + cooldownSec + "s @ " + (rps / 10) + " rps");
        runPhase(cooldownSec, Math.max(1, rps / 10));

        Thread.sleep(5_000);
        verifyQueries();
        System.out.println("DONE sent=" + sent.get() + " ok=" + ok.get() + " err=" + err.get());
        int rc = err.get() > sent.get() * 0.05 ? 1 : 0;
        System.exit(rc);
    }

    private void waitForHealth(String target) throws InterruptedException {
        URI uri = URI.create(target);
        String base = uri.getScheme() + "://" + uri.getAuthority() + "/health";
        for (int i = 0; i < 60; i++) {
            try {
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base)).GET().build(),
                        BodyHandlers.ofString());
                if (r.statusCode() == 200) {
                    System.out.println("Nginx reachable, starting load");
                    return;
                }
            } catch (Exception e) {
                // retry
            }
            Thread.sleep(2000);
        }
        System.err.println("Nginx never came up");
    }

    private void runPhase(int durationSeconds, int targetRps) throws InterruptedException {
        ScheduledExecutorService sch = Executors.newScheduledThreadPool(2);
        long intervalNanos = 1_000_000_000L / Math.max(1, targetRps);
        long end = System.nanoTime() + Duration.ofSeconds(durationSeconds).toNanos();
        Runnable sendOne = () -> sendOne();
        long nextFire = System.nanoTime();
        while (System.nanoTime() < end) {
            sch.submit(sendOne);
            nextFire += intervalNanos;
            long sleep = nextFire - System.nanoTime();
            if (sleep > 0) {
                TimeUnit.NANOSECONDS.sleep(sleep);
            }
        }
        sch.shutdown();
        sch.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void sendOne() {
        try {
            String adId = "ad-" + String.format("%04d", RAND.nextInt(adCardinality));
            ClickEvent e = ClickEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .adId(adId)
                    .userId("u-" + RAND.nextInt(10_000))
                    .countryCode(COUNTRIES[RAND.nextInt(COUNTRIES.length)])
                    .deviceType(DEVICES[RAND.nextInt(DEVICES.length)])
                    .clickTs(Instant.now().toEpochMilli())
                    .build();
            HttpRequest req = HttpRequest.newBuilder(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(2))
                    .header("Content-Type", "application/json")
                    .POST(BodyPublishers.ofByteArray(MAPPER.writeValueAsBytes(e)))
                    .build();
            CompletableFuture<HttpResponse<Void>> f = http.sendAsync(req, BodyHandlers.discarding());
            sent.incrementAndGet();
            f.whenComplete((resp, ex) -> {
                if (ex != null || resp.statusCode() >= 400) {
                    err.incrementAndGet();
                } else {
                    ok.incrementAndGet();
                }
            });
        } catch (Exception ex) {
            err.incrementAndGet();
        }
    }

    private void triggerRecalc(long runStartEpochSec) {
        long now = Instant.now().getEpochSecond();
        String url = recalcUrl + "?from=" + runStartEpochSec + "&to=" + now;
        try {
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder(URI.create(url)).POST(BodyPublishers.noBody()).build(),
                    BodyHandlers.ofString());
            System.out.println("recalc trigger status=" + r.statusCode() + " body=" + r.body());
        } catch (Exception e) {
            System.err.println("recalc trigger failed: " + e.getMessage());
        }
    }

    private void verifyQueries() {
        try {
            HttpResponse<String> top = http.send(
                    HttpRequest.newBuilder(URI.create(queryUrl + "/ads/popular_ads?count=5&window=5&filter=0"))
                            .GET().build(),
                    BodyHandlers.ofString());
            System.out.println("popular_ads status=" + top.statusCode() + " body=" + top.body());
            HttpResponse<String> single = http.send(
                    HttpRequest.newBuilder(URI.create(queryUrl + "/ads/ad-0001/aggregated_count?filter=0"))
                            .GET().build(),
                    BodyHandlers.ofString());
            System.out.println("aggregated_count ad-0001 status=" + single.statusCode() + " body=" + single.body());
        } catch (Exception e) {
            System.err.println("verify failed: " + e.getMessage());
        }
    }

    private void startMetricsServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(metricsPort), 0);
        server.createContext("/metrics", ex -> {
            String body = "# HELP load_client_sent_total Total sent\n" +
                    "# TYPE load_client_sent_total counter\n" +
                    "load_client_sent_total " + sent.get() + "\n" +
                    "# HELP load_client_ok_total Successful responses\n" +
                    "# TYPE load_client_ok_total counter\n" +
                    "load_client_ok_total " + ok.get() + "\n" +
                    "# HELP load_client_err_total Failed responses\n" +
                    "# TYPE load_client_err_total counter\n" +
                    "load_client_err_total " + err.get() + "\n";
            byte[] bytes = body.getBytes();
            ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.getResponseBody().close();
        });
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.start();
    }

    record Config(
            String targetUrl,
            String recalcUrl,
            String queryUrl,
            int rps,
            int warmupSec,
            int durationSec,
            int cooldownSec,
            int adCardinality,
            int metricsPort) {
        static Config fromEnv() {
            return new Config(
                    env("TARGET_URL", "http://localhost/clicks"),
                    env("RECALC_URL", "http://localhost/recalc"),
                    env("QUERY_URL", "http://localhost"),
                    intEnv("RPS", 500),
                    intEnv("WARMUP_SECONDS", 30),
                    intEnv("DURATION_SECONDS", 120),
                    intEnv("COOLDOWN_SECONDS", 30),
                    intEnv("AD_CARDINALITY", 200),
                    intEnv("METRICS_PORT", 9095));
        }

        private static String env(String key, String def) {
            String v = System.getenv(key);
            return v == null || v.isBlank() ? def : v;
        }

        private static int intEnv(String key, int def) {
            try { return Integer.parseInt(env(key, "")); } catch (Exception e) { return def; }
        }
    }
}
