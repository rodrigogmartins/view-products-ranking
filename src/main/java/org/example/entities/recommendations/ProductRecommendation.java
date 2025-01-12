package org.example.entities.recommendations;

import java.util.List;


public record ProductRecommendation(
   String id,
   String name,
   List<String> categories,
   double score
) {}
