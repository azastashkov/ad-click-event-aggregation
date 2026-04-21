package com.acea.agg;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class StreamsRunner implements HealthIndicator {

    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;
    @Value("${app.streams.application-id}")     String applicationId;
    @Value("${app.streams.source-topic}")       String sourceTopic;
    @Value("${app.streams.sink-topic}")         String sinkTopic;
    @Value("${app.streams.num-standbys:1}")     int standbys;
    @Value("${instance.id:local}")              String instanceId;

    private final MeterRegistry meters;
    private KafkaStreams streams;
    private KafkaStreamsMetrics streamsMetrics;

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, standbys);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, applicationId + "-" + instanceId);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, instanceId + ":8080");

        Topology topology = AggregationTopology.build(sourceTopic, sinkTopic);
        log.info("Streams topology:\n{}", topology.describe());

        streams = new KafkaStreams(topology, props);
        streamsMetrics = new KafkaStreamsMetrics(streams);
        streamsMetrics.bindTo(meters);
        streams.setUncaughtExceptionHandler(e -> {
            log.error("Streams uncaught exception", e);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close();
        }
        if (streamsMetrics != null) {
            streamsMetrics.close();
        }
    }

    @Override
    public Health health() {
        if (streams == null) {
            return Health.down().withDetail("state", "not-started").build();
        }
        KafkaStreams.State state = streams.state();
        boolean up = state.isRunningOrRebalancing();
        return (up ? Health.up() : Health.down()).withDetail("state", state.name()).build();
    }
}
