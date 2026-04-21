package com.acea.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class AdIdPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions == 0) {
            return 0;
        }
        String adId = resolveAdId(key, keyBytes);
        return Math.floorMod(adId.hashCode(), numPartitions);
    }

    private static String resolveAdId(Object key, byte[] keyBytes) {
        if (key instanceof String s) {
            return s;
        }
        if (keyBytes != null) {
            return new String(keyBytes, StandardCharsets.UTF_8);
        }
        return "";
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
