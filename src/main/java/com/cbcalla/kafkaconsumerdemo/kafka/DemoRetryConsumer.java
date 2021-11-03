package com.cbcalla.kafkaconsumerdemo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

@Component
public class DemoRetryConsumer extends BaseConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DemoRetryConsumer.class);

  public DemoRetryConsumer() {
    super();
    String failureTopic = "ready-to-sync-dlq";
    String consumer = "ready-to-sync-retry-1-consumer";
    super.Initialize(failureTopic, consumer);
  }

  @SuppressWarnings("rawtypes")
  public Disposable consume() {

    String topic = "ready-to-sync-retry-1";
    ReceiverOptions<Integer, String> options =
        receiverOptions
            .subscription(Collections.singleton(topic))
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
        .flatMap(record -> demoService.process(record.value()).then(Mono.just(record)))
        .doOnNext(record -> record.receiverOffset().acknowledge())
        // .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)).transientErrors(true))
        .onErrorContinue(
            (ex, record) -> {
              LOGGER.info("Sending to retry topic");
              prmRecoverer.accept((ReceiverRecord) record, (Exception) ex);
              ((ReceiverRecord) record).receiverOffset().acknowledge();
            })
        .repeat()
        .subscribe();
  }
}
