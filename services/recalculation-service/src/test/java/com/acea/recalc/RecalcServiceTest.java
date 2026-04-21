package com.acea.recalc;

import com.acea.common.model.ClickEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecalcServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void runBlockingPublishesAllEvents() {
        RawClickReader reader = mock(RawClickReader.class);
        KafkaTemplate<String, ClickEvent> kafka = mock(KafkaTemplate.class);

        List<ClickEvent> events = List.of(
                ClickEvent.builder().eventId("e1").adId("ad-1").userId("u1")
                        .countryCode("US").deviceType("DESKTOP").clickTs(1000L).build(),
                ClickEvent.builder().eventId("e2").adId("ad-2").userId("u2")
                        .countryCode("DE").deviceType("MOBILE").clickTs(1000L).build());
        Iterator<ClickEvent> iter = events.iterator();

        when(reader.scanRange(anyLong(), anyLong(), anyInt())).thenReturn(iter);
        ProducerRecord<String, ClickEvent> record = new ProducerRecord<>("t", "k", null);
        RecordMetadata md = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0, 0, 0);
        SendResult<String, ClickEvent> sr = new SendResult<>(record, md);
        when(kafka.send(anyString(), anyString(), any(ClickEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sr));

        RecalcService svc = new RecalcService(reader, kafka, new SimpleMeterRegistry());
        long sent = svc.runBlocking(0, 9999, 500);
        assertThat(sent).isEqualTo(2);
        assertThat(svc.isRunning()).isFalse();
    }
}
