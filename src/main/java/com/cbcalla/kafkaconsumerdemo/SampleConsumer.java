package com.cbcalla.kafkaconsumerdemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class SampleConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SampleConsumer.class);

  private final MyFirstRetryRecoverer retryRecoverer;
  private final DeadLetterPublishingRecoverer dlqRecoverer;

  private final ReceiverOptions<Integer, String> receiverOptions;
  private final ReceiverOptions<Integer, String> receiverOptionsRetry;

  private static final String BOOTSTRAP_SERVERS = "localhost:29092";

  private final String topic = "ready-to-sync";
  private final String topicRetry = "ready-to-sync-retry-1";
  private final String topicDlq = "ready-to-sync-dlq";

  public SampleConsumer() {

    retryRecoverer =
        new MyFirstRetryRecoverer(
            getEventKafkaTemplate(), (record, ex) -> new TopicPartition(topicRetry, -1));

    dlqRecoverer =
        new DeadLetterPublishingRecoverer(
            getEventKafkaTemplate(), (record, ex) -> new TopicPartition(topicDlq, -1));

    Map<String, Object> props = new HashMap<>();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    receiverOptions = ReceiverOptions.create(props);

    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer-retry");
    receiverOptionsRetry = ReceiverOptions.create(props);
  }

  private KafkaOperations<String, Object> getEventKafkaTemplate() {
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()));
  }

  Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  public Disposable consumeMessages() {

    ReceiverOptions<Integer, String> options =
        receiverOptions
            .subscription(Collections.singleton(topic))
            .addAssignListener(partitions -> LOGGER.debug("onPartitionsAssigned {}", partitions))
            .addRevokeListener(partitions -> LOGGER.debug("onPartitionsRevoked {}", partitions));

    return KafkaReceiver.create(options)
        .receive()
        .doOnNext(
            record -> {
              LOGGER.info(
                  record.offset() + "@" + record.topic() + " [" + record.value() + "]");
              if (record.value().contains("fail")) {
                throw new ReceiverRecordException(record, new RuntimeException("oops!!"));
              }
              record.receiverOffset().acknowledge();
            })
        .doOnError(System.out::println)
        .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)).transientErrors(true))
        .onErrorContinue(
            (e, record) -> {
              ReceiverRecordException ex = (ReceiverRecordException) e;
              LOGGER.info("Sending to retry topic " + ex);
              retryRecoverer.accept(ex.getRecord(), ex);
              ex.getRecord().receiverOffset().acknowledge();
            })
        .repeat()
        .subscribe();
  }

  public Disposable consumeMessagesRetry() {

    ReceiverOptions<Integer, String> options =
        receiverOptionsRetry
            .subscription(Collections.singleton(topicRetry))
            .addAssignListener(partitions -> LOGGER.debug("onPartitionsAssigned {}", partitions))
            .addRevokeListener(partitions -> LOGGER.debug("onPartitionsRevoked {}", partitions));

    return KafkaReceiver.create(options)
        .receive()
        .flatMap(
            record -> {
              var currentTimeEpochMilli = Instant.now().toEpochMilli();
              if (currentTimeEpochMilli - record.timestamp() < 15000L) {
                LOGGER.info("Delaying retry for 15 seconds");
                return Mono.just(record).delayElement(Duration.ofSeconds(15));
              }
              return Mono.just(record);
            })
        .doOnNext(
            record -> {
              LOGGER.info(
                  record.offset() + "@" + record.topic() + " [" + record.value() + "]");
              if (record.value().contains("epic")) {
                throw new ReceiverRecordException(record, new RuntimeException("oh crap!"));
              }
              record.receiverOffset().acknowledge();
            })
        .doOnError(System.out::println)
        .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)).transientErrors(true))
        .onErrorContinue(
            (e, record) -> {
              ReceiverRecordException ex = (ReceiverRecordException) e;
              LOGGER.info("Retries exhausted for " + ex);
              dlqRecoverer.accept(ex.getRecord(), ex);
              ex.getRecord().receiverOffset().acknowledge();
            })
        .repeat()
        .subscribe();
  }
}
