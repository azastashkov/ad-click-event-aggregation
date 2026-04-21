package com.acea.aggwriter;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.util.Arrays;

@Configuration
public class CassandraConfig {

    @Bean(destroyMethod = "close")
    public CqlSession cqlSession(
            @Value("${cassandra.contact-points}") String contactPoints,
            @Value("${cassandra.local-dc:dc1}") String localDc) {
        CqlSessionBuilder builder = CqlSession.builder().withLocalDatacenter(localDc);
        Arrays.stream(contactPoints.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .forEach(hp -> {
                    String[] parts = hp.split(":");
                    builder.addContactPoint(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                });
        return builder.build();
    }
}
