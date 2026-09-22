package com.example.seat.consumer;

import com.example.seat.producer.SeatConfirmedProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class BookingEventConsumer {

    private final SeatConfirmedProducer seatConfirmedProducer;

    public BookingEventConsumer(SeatConfirmedProducer seatConfirmedProducer) {
        this.seatConfirmedProducer = seatConfirmedProducer;
    }

    @KafkaListener(topics = "booking-events", groupId = "seat-group")
    public void handleBooking(ConsumerRecord<String, String> record) {
        String correlationId = new String(record.headers().lastHeader("correlationId").value(), StandardCharsets.UTF_8);
        
        System.out.println("[SeatAllocationService] Received SeatRequest for " + record.key() + ". CorrelationID: " + correlationId);
        System.out.println("[SeatAllocationService] Seat reserved: A12, A13. CorrelationID: " + correlationId);

        seatConfirmedProducer.publishSeatConfirmed(null, correlationId);
    }
}