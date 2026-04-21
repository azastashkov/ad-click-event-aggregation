package com.acea.watcher;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> commonTags(
            @Value("${instance.id:local}") String instanceId) {
        return registry -> registry.config()
                .meterFilter(MeterFilter.commonTags(
                        io.micrometer.core.instrument.Tags.of("service", "click-event-watcher",
                                "instance", instanceId)));
    }
}
