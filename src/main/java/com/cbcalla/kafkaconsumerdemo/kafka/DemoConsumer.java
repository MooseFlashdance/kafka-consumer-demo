package com.cbcalla.kafkaconsumerdemo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@Component
public class DemoConsumer extends BaseConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DemoConsumer.class);

  public DemoConsumer() {

    super();

    final String topic = "ready-to-sync";
    final String failureTopic = "ready-to-sync-retry-1";
    final String consumer = "ready-to-sync-consumer";

    super.Initialize(topic, failureTopic, consumer);
  }

  @SuppressWarnings("rawtypes")
  public Disposable consume() {

    return KafkaReceiver.create(super.receiverOptions)
        .receive()
        .flatMap(record -> demoService.process(record.value()).then(Mono.just(record)))
        .doOnNext(record -> record.receiverOffset().acknowledge())
        //  .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)).transientErrors(true))
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
