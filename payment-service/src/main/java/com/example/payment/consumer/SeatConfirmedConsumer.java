package com.example.payment.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class SeatConfirmedConsumer {

    @KafkaListener(topics = "seat-confirmed-events", groupId = "payment-group")
    public void handleSeatConfirmed(ConsumerRecord<String, String> record) {
        String correlationId = new String(record.headers().lastHeader("correlationId").value(), StandardCharsets.UTF_8);
        
        System.out.println("[PaymentService] Processing Payment for " + record.key() + ". CorrelationID: " + correlationId);
        System.out.println("[PaymentService] Payment success: 240000 VND. CorrelationID: " + correlationId);
    }
}