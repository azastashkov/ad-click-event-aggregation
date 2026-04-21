package com.acea.query;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/ads")
public class QueryController {

    private final QueryService service;

    @GetMapping("/{adId}/aggregated_count")
    public Map<String, Object> aggregatedCount(
            @PathVariable String adId,
            @RequestParam(name = "from", required = false) Long from,
            @RequestParam(name = "to", required = false) Long to,
            @RequestParam(name = "filter", required = false, defaultValue = "0") int filter) {
        long toMinute = to == null ? QueryService.nowMinute() : to;
        long fromMinute = from == null ? toMinute - 1 : from;
        long count = service.aggregatedCount(adId, filter, fromMinute, toMinute);
        return Map.of("ad_id", adId, "count", count,
                "from", fromMinute, "to", toMinute, "filter", filter);
    }

    @GetMapping("/popular_ads")
    public Map<String, Object> popularAds(
            @RequestParam(name = "count", required = false, defaultValue = "10") int count,
            @RequestParam(name = "window", required = false, defaultValue = "5") int window,
            @RequestParam(name = "filter", required = false, defaultValue = "0") int filter) {
        List<String> ads = service.popularAds(count, window, filter);
        return Map.of("ad_ids", ads, "count", ads.size(),
                "window", window, "filter", filter);
    }
}
