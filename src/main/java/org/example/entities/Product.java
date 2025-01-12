package org.example.entities;

import java.util.List;

public record Product(
    String id,
    String name,
    double price,
    List<String> categories
) {}
