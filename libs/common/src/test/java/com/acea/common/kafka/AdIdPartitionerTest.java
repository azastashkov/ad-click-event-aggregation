package com.acea.common.kafka;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AdIdPartitionerTest {

    @Test
    void sameAdIdMapsToSamePartition() {
        AdIdPartitioner p = new AdIdPartitioner();
        Cluster cluster = clusterWith(6);
        int first = p.partition("t", "ad-42", "ad-42".getBytes(), null, null, cluster);
        int second = p.partition("t", "ad-42", "ad-42".getBytes(), null, null, cluster);
        assertThat(first).isEqualTo(second);
        assertThat(first).isBetween(0, 5);
    }

    @Test
    void differentAdIdsSpreadAcrossPartitions() {
        AdIdPartitioner p = new AdIdPartitioner();
        Cluster cluster = clusterWith(6);
        int[] distribution = new int[6];
        for (int i = 0; i < 500; i++) {
            String adId = "ad-" + i;
            distribution[p.partition("t", adId, adId.getBytes(), null, null, cluster)]++;
        }
        for (int slot : distribution) {
            assertThat(slot).isGreaterThan(30);
        }
    }

    @Test
    void noPartitionsYieldsZero() {
        AdIdPartitioner p = new AdIdPartitioner();
        Cluster cluster = new Cluster("cid", Collections.emptyList(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet());
        assertThat(p.partition("t", "ad-1", "ad-1".getBytes(), null, null, cluster)).isZero();
    }

    private static Cluster clusterWith(int numPartitions) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> partitions = new java.util.ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new PartitionInfo("t", i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("cid", List.of(node), partitions, Collections.emptySet(), Collections.emptySet());
    }
}
