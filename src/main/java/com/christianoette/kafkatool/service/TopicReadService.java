package com.christianoette.kafkatool.service;

import com.christianoette.kafkatool.web.MessageReceivedEvent;
import lombok.Getter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class TopicReadService {

    private final KafkaProperties kafkaProperties;
    private final ApplicationEventPublisher events;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    @Getter
    private volatile boolean running = false;
    private KafkaConsumer<String, String> consumer;

    public TopicReadService(KafkaProperties kafkaProperties, ApplicationEventPublisher events) {
        this.kafkaProperties = kafkaProperties;
        this.events = events;
    }

    public List<String> getAllTopicNames() {
        String bootstrap = String.join(",", kafkaProperties.getBootstrapServers());
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        if (kafkaProperties.getProperties() != null) {
            kafkaProperties.getProperties().forEach(props::put);
        }

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> names = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            return names.stream().sorted().toList();
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public synchronized boolean connect() {
        if (running) {
            return false;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafkaProperties.getBootstrapServers()));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer() != null && kafkaProperties.getConsumer().getGroupId() != null
                ? kafkaProperties.getConsumer().getGroupId()
                : "kafkatool-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        if (kafkaProperties.getProperties() != null) {
            kafkaProperties.getProperties().forEach(props::put);
        }
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile(".*"));
        running = true;
        executor.submit(() -> {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(rec -> {
                    String headers = rec.headers() == null ? "" :
                            java.util.stream.StreamSupport.stream(rec.headers().spliterator(), false)
                                    .map(h -> h.key() + "=" + (h.value() == null ? "null" : new String(h.value())))
                                    .collect(Collectors.joining(","));
                    String body = rec.value() == null ? "" : rec.value().replaceAll("\n|\r", " ");
                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss.SSSSSS", java.util.Locale.GERMANY);
                    String ts = fmt.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(rec.timestamp()), ZoneId.systemDefault()));
                    events.publishEvent(new MessageReceivedEvent(rec.topic(), headers, body, ts));
                });
            }
        });
        return true;
    }

    public synchronized void disconnect() {
        running = false;
        if (consumer != null) {
            try { consumer.wakeup(); } catch (Exception ignored) {}
            try { consumer.close(Duration.ofSeconds(2)); } catch (Exception ignored) {}
            consumer = null;
        }
    }

    @PreDestroy
    void stopReadingAllTopics() {
        disconnect();
        executor.shutdownNow();
    }
}
