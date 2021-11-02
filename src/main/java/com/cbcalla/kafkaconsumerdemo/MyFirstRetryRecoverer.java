package com.cbcalla.kafkaconsumerdemo;

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

public class MyFirstRetryRecoverer extends DeadLetterPublishingRecoverer {

  public MyFirstRetryRecoverer(
      KafkaOperations<?, ?> template,
      BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {

    super(template, destinationResolver);
  }

  @Override
  protected ProducerRecord<Object, Object> createProducerRecord(
      @NonNull ConsumerRecord<?, ?> record,
      @NonNull TopicPartition topicPartition,
      @NonNull Headers headers,
      @Nullable byte[] key,
      @Nullable byte[] value) {

    headers.add("retry-count", "1".getBytes(StandardCharsets.UTF_8));

    return super.createProducerRecord(record, topicPartition, headers, key, value);
  }
}
