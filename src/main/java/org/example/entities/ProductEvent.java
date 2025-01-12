package org.example.entities;

public record ProductEvent(
    String productId,
    String eventType,
    String userId,
    Long timestamp
) {}
