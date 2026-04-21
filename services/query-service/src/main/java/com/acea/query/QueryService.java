package com.acea.query;

import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class QueryService {

    private final AggregateQueryRepository repo;

    public static long nowMinute() {
        return Instant.now().getEpochSecond() / 60;
    }

    @Cacheable(cacheNames = "adCounts", key = "#adId + ':' + #filterId + ':' + #fromMinute + ':' + #toMinute")
    public long aggregatedCount(String adId, int filterId, long fromMinute, long toMinute) {
        return repo.sumForAd(adId, filterId, fromMinute, toMinute);
    }

    @Cacheable(cacheNames = "popularAds", key = "#count + ':' + #windowMinutes + ':' + #filterId")
    public List<String> popularAds(int count, int windowMinutes, int filterId) {
        long to = nowMinute();
        long from = to - Math.max(1, windowMinutes) + 1;
        List<AggregateQueryRepository.AdCount> top = repo.topForWindow(filterId, from, to, count);
        return top.stream().map(AggregateQueryRepository.AdCount::getAdId).toList();
    }
}
