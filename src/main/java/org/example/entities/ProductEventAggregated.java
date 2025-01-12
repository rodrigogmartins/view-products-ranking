package org.example.entities;

public record ProductEventAggregated(
    String productId,
    String eventType,
    int counts
) {}
