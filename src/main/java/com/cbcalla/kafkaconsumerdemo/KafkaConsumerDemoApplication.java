package com.cbcalla.kafkaconsumerdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.Disposable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaConsumerDemoApplication implements CommandLineRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerDemoApplication.class);


  @Autowired SampleConsumer consumer;

  public static void main(String[] args) {
    SpringApplication.run(KafkaConsumerDemoApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    LOGGER.info("Starting");
    CountDownLatch latch = new CountDownLatch(60);
    Disposable disposable = consumer.consumeMessages();
    Disposable disposable1 = consumer.consumeMessagesRetry();
    latch.await(300, TimeUnit.SECONDS);
    disposable.dispose();
    LOGGER.info("Complete");
  }
}
