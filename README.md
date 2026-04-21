# Ad Click Event Aggregation System

A production-style, horizontally-scalable ad-click event pipeline built with Java 21, Spring Boot 3, Groovy Gradle, Lombok, Apache Kafka (on ZooKeeper), Kafka Streams, and Apache Cassandra.

```
clients → nginx → click-event-watcher → kafka → ┬── raw-database-writer     → cassandra.raw
                                                └── aggregation-service     → kafka (aggregated-counts)
                                                                                 └── aggregation-database-writer → cassandra.aggregates
                                                                                                                    ↑
         ↳ query-service (behind nginx) reads ←┘                                                                    │
                                                                                                                     │
                                                                                          recalculation-service ─────┘
                                                                                          (Cassandra.raw → recalc topics → same DAG)
```

See [`docs/architecture.drawio`](docs/architecture.drawio) for a full component diagram (open in [draw.io](https://app.diagrams.net/)).

## Requirements

- Docker + docker-compose v2
- Java 21 (for local build) — the provided Jib builds push images directly into the local Docker daemon.
- Gradle 8.10 (or run `gradle wrapper` once to generate the wrapper).

## Repository layout

```
libs/common/                      Shared DTOs, JSON serdes, Kafka partitioner, Streams topology
services/click-event-watcher/     REST → Kafka producer (partitioned by ad_id)
services/raw-database-writer/     Kafka consumer → Cassandra raw.ad_clicks
services/aggregation-service/     Kafka Streams DAG (Source → Map → Aggregate → Sink)
services/aggregation-database-writer/  Kafka consumer → Cassandra aggregates.*
services/query-service/           REST: aggregated_count & popular_ads
services/recalculation-service/   Cassandra raw → recalc topics → dedicated Streams app
tools/load-test-client/           Java load generator (profile=test)
infra/                            nginx, prometheus, grafana, cassandra schema
docker-compose.yml                All infra + app containers on one bridge network
```

## Building images

```bash
# Optional: generate the Gradle wrapper (first time only)
gradle wrapper --gradle-version 8.10

# Build + push images directly into the local Docker daemon via Jib
./gradlew clean jibDockerBuild
```

## Running the stack

```bash
docker compose up -d
# First boot takes ~60-90s while Cassandra forms the cluster and topics are created.

# Wait for healthy services
docker compose ps
```

Key ports exposed on the host:

| Port | Service |
| --- | --- |
| 80 | Nginx (public ingress: `/clicks`, `/ads/...`, `/recalc`) |
| 3000 | Grafana (admin/admin) |
| 9090 | Prometheus |
| 9000 | ZooNavigator (connection `acea` pre-configured) |
| 19092-19094 | External Kafka broker listeners |
| 8081 | Nginx stub_status |

## Using the APIs

Ingest a click:

```bash
# JSON body
curl -i -X POST http://localhost/clicks \
  -H 'Content-Type: application/json' \
  -d '{"adId":"ad-0001","userId":"u1","countryCode":"US","deviceType":"MOBILE"}'

# GET form (handy for the browser / wget)
curl "http://localhost/clicks?adId=ad-0001&countryCode=US&deviceType=MOBILE"
```

Query aggregated count for an ad:

```bash
# Default: from = now-1min, to = now, filter = 0 (none)
curl "http://localhost/ads/ad-0001/aggregated_count"

# Explicit minute range + US-only filter (filter=1)
NOW=$(( $(date +%s) / 60 ))
curl "http://localhost/ads/ad-0001/aggregated_count?from=$((NOW-5))&to=$NOW&filter=1"
```

Query top-N popular ads:

```bash
curl "http://localhost/ads/popular_ads?count=10&window=5&filter=0"
```

Trigger a recalculation (historical replay from raw data):

```bash
FROM=$(( $(date +%s) - 3600 ))
TO=$(date +%s)
curl -i -X POST "http://localhost/recalc?from=$FROM&to=$TO"
```

### Filter IDs

| filter | meaning |
| --- | --- |
| 0 | NONE — aggregates all clicks |
| 1 | US_ONLY — `countryCode == "US"` |
| 2 | MOBILE_ONLY — `deviceType == "MOBILE"` |

Aggregations are pre-computed at ingest time for every applicable filter, so reads are single-partition lookups.

## Load testing

The load client ships as a separate container gated by the `test` compose profile:

```bash
docker compose --profile test up --exit-code-from load-client load-client
```

Workload (configurable via env vars in `docker-compose.yml`):

1. **Warm-up** 30 s at `rps/4`.
2. **Sustained** 120 s at `rps` (default 500 rps). Fires one `POST /recalc` mid-run to exercise the Recalculation Service.
3. **Cool-down** 30 s at `rps/10`.
4. Queries `popular_ads` and `aggregated_count` to sanity-check the pipeline.

The client exits with code 0 when the error rate stays under 5 %, non-zero otherwise. Its own metrics are scraped by Prometheus on `:9095/metrics`.

## Observability

- Prometheus scrapes `/actuator/prometheus` on every service. Configuration: [`infra/prometheus/prometheus.yml`](infra/prometheus/prometheus.yml).
- Grafana provisions the "Ad Click Pipeline" dashboard automatically. Panels:
  - Watcher accepted RPS, HTTP latencies
  - Kafka producer & consumer rates / lag
  - Raw & Aggregation writer throughput
  - Query service RPS + p95
  - Recalculation output rate
  - Load-client sent/ok/err
  - Kafka Streams processing rate

## Data model

Cassandra keyspaces (replication factor 3 on `dc1`):

**`raw.ad_clicks`** — partition key `(ad_id, bucket_hour)`, clustered by `click_ts DESC, event_id ASC`. Bucketing by epoch-hour bounds partition size.

**`aggregates.by_ad`** — partition key `(ad_id, filter_id)`, clustered by `minute DESC`. Answers API 1 (aggregated_count per ad, filter, minute range).

**`aggregates.by_bucket`** — partition key `(filter_id, minute)`, clustered by `count DESC, ad_id ASC`. Answers API 2 (popular_ads — top-N per window/filter) with a single-partition scan per minute.

**`aggregates.last_count`** — tracks the previous count per `(filter_id, minute, ad_id)` so the writer can evict stale `by_bucket` rows when Kafka Streams emits an updated aggregate (since `by_bucket` has `count` in its primary key).

The full schema lives in [`infra/cassandra/schema.cql`](infra/cassandra/schema.cql).

## Streaming DAG

The aggregation service builds this Kafka Streams topology (shared code lives in `com.acea.common.streams.AggregationTopology`):

```
Source: stream("ad-click-events")
   │
   ▼ Map (flatMap): one record per applicable filter, keyed "adId|minute|filterId"
   │
   ▼ GroupByKey
   │
   ▼ Aggregate (1-minute tumbling window, 5-minute grace)
   │
   ▼ Reduce-equivalent (mapValues to AggregatedCount)
   │
Sink: to("aggregated-counts")
```

The same topology is reused by the Recalculation Service with different source/sink topics (`recalc-ad-click-events` → `recalc-aggregated-counts`) and a different Streams `application.id` (`agg-recalc`), so historical replay is fully isolated from the real-time path.

## Tests

```bash
./gradlew test
```

Per-module coverage:
- `libs:common` — `AdIdPartitioner`, `FilterSpec`.
- `click-event-watcher` — controller happy path + invalid adId.
- `raw-database-writer` — bucket math + UUID parsing.
- `aggregation-service` — `TopologyTestDriver` asserts filter fan-out and windowed summing.
- `aggregation-database-writer` — repository upserts three tables, deletes stale `by_bucket` rows on count change.
- `query-service` — service + controller defaults.
- `recalculation-service` — end-to-end blocking run using a mocked reader and Kafka template.

## Troubleshooting

- **Cassandra unhealthy** on first boot — the 3-node cluster takes ~45 s to converge. `cassandra-init` waits for healthchecks before applying the schema.
- **Topics missing** — re-run `docker compose up kafka-init` to reapply the topic configs (idempotent).
- **No data in Grafana** — check `http://localhost:9090/targets`; every job should be `UP`.
- **ZooNavigator** — open `http://localhost:9000`; the `acea` connection auto-connects to `zk-1:2181,zk-2:2181,zk-3:2181`.

## High availability

All application services run with 2 replicas (recalc-service runs 1 — it's a leader-owned batch job). Kafka runs 3 brokers with RF=3 & `min.insync.replicas=2`. Cassandra runs 3 nodes with NetworkTopologyStrategy RF=3. Nginx fronts the watcher and query services with `least_conn` load balancing plus passive health checks.

## License

Internal reference project.
