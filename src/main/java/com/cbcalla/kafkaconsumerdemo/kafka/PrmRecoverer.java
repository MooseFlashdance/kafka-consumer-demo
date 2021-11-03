package com.cbcalla.kafkaconsumerdemo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;

/** Extending DeadLetterPublishingRecoverer so we can customize the Kafka headers */
public class PrmRecoverer extends DeadLetterPublishingRecoverer {

  private static final String FAILURE_COUNT = "failure-count";

  public PrmRecoverer(
      KafkaOperations<?, ?> template,
      BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {

    super(template, destinationResolver);
  }

  /**
   * Increments the retry-count header and then calls the super to set the rest of the failure info
   * in the headers
   *
   * @param record The consumer record that failed to process
   * @param topicPartition The partition the record came from
   * @param headers The headers
   * @param key The record key
   * @param value The record value
   * @return A producer record for the retry or dead letter queue
   */
  @Override
  protected ProducerRecord<Object, Object> createProducerRecord(
      @NonNull ConsumerRecord<?, ?> record,
      @NonNull TopicPartition topicPartition,
      @NonNull Headers headers,
      @Nullable byte[] key,
      @Nullable byte[] value) {

    var failureCount = 0;

    try {
      if (headers.lastHeader(FAILURE_COUNT) != null) {
        var failureCountAsString =
            new String(headers.lastHeader(FAILURE_COUNT).value(), StandardCharsets.UTF_8);
        failureCount = Integer.parseInt(failureCountAsString);
      }
    } catch (NumberFormatException nfe) {
      // ignore a non-numeric value
    }

    headers.remove(FAILURE_COUNT);

    failureCount++;
    headers.add(FAILURE_COUNT, Integer.toString(failureCount).getBytes(StandardCharsets.UTF_8));

    return super.createProducerRecord(record, topicPartition, headers, key, value);
  }
}
