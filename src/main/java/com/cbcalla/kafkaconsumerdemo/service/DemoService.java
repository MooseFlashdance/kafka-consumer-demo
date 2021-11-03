package com.cbcalla.kafkaconsumerdemo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class DemoService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DemoService.class);

  public Mono<Boolean> process(String message) {

    LOGGER.info(message);

    if (message.contains("fail")) {
      throw new IllegalStateException("Failure Detected!");
    }

    return Mono.just(true);
  }
}
