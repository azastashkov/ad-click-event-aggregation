package com.acea.rawwriter;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RawClickRepositoryTest {

    @Test
    void bucketHourDivides3600() {
        long now = Instant.parse("2026-04-20T10:30:15Z").toEpochMilli();
        long bucket = RawClickRepository.bucketHour(now);
        // Hours-since-epoch of 2026-04-20T10:00:00Z
        long expected = Instant.parse("2026-04-20T10:00:00Z").getEpochSecond() / 3600;
        assertThat(bucket).isEqualTo(expected);
    }

    @Test
    void parseUuidReturnsRandomOnBadInput() {
        UUID u = RawClickRepository.parseUuid("not-a-uuid");
        assertThat(u).isNotNull();
    }

    @Test
    void parseUuidAcceptsValid() {
        String s = UUID.randomUUID().toString();
        assertThat(RawClickRepository.parseUuid(s).toString()).isEqualTo(s);
    }
}
