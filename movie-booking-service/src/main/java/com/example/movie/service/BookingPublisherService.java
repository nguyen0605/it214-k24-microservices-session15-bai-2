package com.example.movie.service;

import com.example.movie.model.CinemaBookingRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
@Slf4j
public class BookingPublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public BookingPublisherService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void createBooking(CinemaBookingRequest request) {
        String correlationId = UUID.randomUUID().toString();
        System.out.println("[MovieBookingService] Created booking " + request.getCinemaBookingId() + ". CorrelationID: " + correlationId);

        try {
            String payload = objectMapper.writeValueAsString(request);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "booking-events", 
                request.getCinemaBookingId(), 
                payload
            );
            record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
        } catch (JsonProcessingException e) {
            System.err.println("Error serializing booking request: " + e.getMessage());
        }
    }
}