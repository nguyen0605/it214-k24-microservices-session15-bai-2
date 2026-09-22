package com.example.seat.producer;

import com.example.seat.model.SeatAllocationEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class SeatConfirmedProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public SeatConfirmedProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishSeatConfirmed(Object request, String correlationId) {
        try {
            SeatAllocationEvent event = new SeatAllocationEvent();
            // Map request data to event if needed
            String payload = objectMapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "seat-confirmed-events", 
                "CIN-2024-789", 
                payload
            );
            record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
        } catch (Exception e) {
            System.err.println("Error publishing seat confirmed: " + e.getMessage());
        }
    }
}