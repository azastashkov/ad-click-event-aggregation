CREATE KEYSPACE IF NOT EXISTS raw
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

CREATE KEYSPACE IF NOT EXISTS aggregates
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

CREATE TABLE IF NOT EXISTS raw.ad_clicks (
    ad_id text,
    bucket_hour bigint,
    click_ts timestamp,
    event_id uuid,
    user_id text,
    country_code text,
    device_type text,
    PRIMARY KEY ((ad_id, bucket_hour), click_ts, event_id)
) WITH CLUSTERING ORDER BY (click_ts DESC, event_id ASC);

-- API 1: aggregated_count for (ad_id, filter, minute range).
CREATE TABLE IF NOT EXISTS aggregates.by_ad (
    ad_id text,
    filter_id int,
    minute bigint,
    count bigint,
    PRIMARY KEY ((ad_id, filter_id), minute)
) WITH CLUSTERING ORDER BY (minute DESC);

-- API 2: popular_ads (top-N per filter/minute) — clustered by count DESC.
CREATE TABLE IF NOT EXISTS aggregates.by_bucket (
    filter_id int,
    minute bigint,
    count bigint,
    ad_id text,
    PRIMARY KEY ((filter_id, minute), count, ad_id)
) WITH CLUSTERING ORDER BY (count DESC, ad_id ASC);

-- Index to rewrite/replace entries for a given minute during re-aggregation without unbounded growth.
CREATE TABLE IF NOT EXISTS aggregates.last_count (
    filter_id int,
    minute bigint,
    ad_id text,
    count bigint,
    PRIMARY KEY ((filter_id, minute), ad_id)
);
