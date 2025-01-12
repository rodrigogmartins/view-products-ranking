package org.example;

import org.example.entities.ProductEvent;

import java.util.List;
import java.util.Random;

public class EventsMock {

    // Generate 3 events of VIEW for each event of BUY (approx.)
    private static final List<String> productEventsListMock = List.of(
        "buy",
        "view",
        "view",
        "view"
    );
    private static final List<String> userIdsListMock = List.of(
        "123e4567-e89b-12d3-a456-426614174000",
        "123e4567-e89b-12d3-a456-426614174001",
        "123e4567-e89b-12d3-a456-426614174002",
        "123e4567-e89b-12d3-a456-426614174003",
        "123e4567-e89b-12d3-a456-426614174004",
        "123e4567-e89b-12d3-a456-426614174005",
        "123e4567-e89b-12d3-a456-426614174006",
        "123e4567-e89b-12d3-a456-426614174007",
        "123e4567-e89b-12d3-a456-426614174008",
        "123e4567-e89b-12d3-a456-426614174009"
    );

    public static ProductEvent getRandomProductEvent() {
        Random random = new Random();
        String productId = (random.nextInt(20) + 1)+"";

        int eventIndex = random.nextInt(productEventsListMock.size());
        String event = productEventsListMock.get(eventIndex);

        int userIdIndex = random.nextInt(userIdsListMock.size());
        String userId = userIdsListMock.get(userIdIndex);

        Long timestamp = System.currentTimeMillis();

        return new ProductEvent(productId, event, userId, timestamp);
    }
}
