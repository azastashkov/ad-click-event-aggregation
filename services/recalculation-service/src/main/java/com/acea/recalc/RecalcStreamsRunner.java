package com.acea.recalc;

import com.acea.common.Topics;
import com.acea.common.streams.AggregationTopology;
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
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class RecalcStreamsRunner {

    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;
    @Value("${instance.id:local}")              String instanceId;

    private final MeterRegistry meters;
    private KafkaStreams streams;
    private KafkaStreamsMetrics metricsBinder;

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-recalc");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "agg-recalc-" + instanceId);

        Topology topology = AggregationTopology.build(
                Topics.RECALC_AD_CLICK_EVENTS, Topics.RECALC_AGGREGATED_COUNTS, "recalc-agg-store");
        log.info("Recalc Streams topology:\n{}", topology.describe());

        streams = new KafkaStreams(topology, props);
        metricsBinder = new KafkaStreamsMetrics(streams);
        metricsBinder.bindTo(meters);
        streams.start();
    }

    @PreDestroy
    public void stop() {
        if (streams != null) streams.close();
        if (metricsBinder != null) metricsBinder.close();
    }
}
