package com.cbcalla.kafkaconsumerdemo.kafka;

import com.cbcalla.kafkaconsumerdemo.service.DemoService;
import jdk.jshell.spi.ExecutionControl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.Disposable;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseConsumer {

  @Autowired DemoService demoService;

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseConsumer.class);

  protected PrmRecoverer prmRecoverer;

  protected ReceiverOptions<Integer, String> receiverOptions;

  private static final String BOOTSTRAP_SERVERS = "localhost:29092";

  public void Initialize(String topic, String failureTopic, String consumer) {

    prmRecoverer =
        new PrmRecoverer(
            getEventKafkaTemplate(), (record, ex) -> new TopicPartition(failureTopic, -1));

    prmRecoverer.setReplaceOriginalHeaders(true);

    Map<String, Object> props = new HashMap<>();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

    receiverOptions = ReceiverOptions.create(props);

    receiverOptions
        .subscription(Collections.singleton(topic))
        .addAssignListener(partitions -> LOGGER.debug("onPartitionsAssigned {}", partitions))
        .addRevokeListener(partitions -> LOGGER.debug("onPartitionsRevoked {}", partitions));
  }

  private KafkaOperations<String, Object> getEventKafkaTemplate() {
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()));
  }

  Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  protected String getRecordLogMessage(ReceiverRecord<Integer, String> record) {
    return record.offset() + "@" + record.topic() + " [" + record.value() + "]";
  }

  private Disposable consume() throws ExecutionControl.NotImplementedException {
    throw new ExecutionControl.NotImplementedException("Must implement in sub-class");
  }
}
