package com.acea.watcher;

import com.acea.common.model.ClickEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClickControllerTest {

    @Test
    @SuppressWarnings("unchecked")
    void postPublishesKafkaEvent() throws Exception {
        KafkaTemplate<String, ClickEvent> kafka = mock(KafkaTemplate.class);
        ProducerRecord<String, ClickEvent> record = new ProducerRecord<>("ad-click-events", "ad-1", null);
        RecordMetadata md = new RecordMetadata(new TopicPartition("ad-click-events", 0), 0, 0, 0, 0, 0);
        SendResult<String, ClickEvent> result = new SendResult<>(record, md);
        when(kafka.send(anyString(), anyString(), any(ClickEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        ClickController controller = new ClickController(kafka, new SimpleMeterRegistry());
        ResponseEntity<Map<String, String>> resp = controller.postClick(ClickEvent.builder()
                .adId("ad-1").userId("u1").countryCode("US").deviceType("MOBILE").clickTs(1L).build());
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).containsKey("eventId");
    }

    @Test
    @SuppressWarnings("unchecked")
    void missingAdIdIsBadRequest() {
        KafkaTemplate<String, ClickEvent> kafka = mock(KafkaTemplate.class);
        ClickController controller = new ClickController(kafka, new SimpleMeterRegistry());
        ResponseEntity<Map<String, String>> resp = controller.postClick(ClickEvent.builder().build());
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
    }
}
