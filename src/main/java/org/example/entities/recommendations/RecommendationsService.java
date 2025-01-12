package org.example.entities.recommendations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RecommendationsService {

    private final HttpClient client;
    private final ObjectMapper mapper;

    public RecommendationsService() {
        this.client = HttpClient.newHttpClient();
        this.mapper = new ObjectMapper();
    }

    public List<ProductRecommendation> getRecommendations(String query) {
        try {
            String encodedParamValue = URLEncoder.encode(query, StandardCharsets.UTF_8);
            URI uri = URI.create("http://localhost:5000/search?query=" + encodedParamValue);
            HttpRequest request = HttpRequest.newBuilder().uri(uri).GET().build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException();
            }

            return mapper.readValue(response.body(), new TypeReference<List<ProductRecommendation>>(){});
        } catch (Exception e) {
            System.err.println("Error calling recommendations API: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
