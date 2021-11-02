package com.cbcalla.kafkaconsumerdemo;

import reactor.kafka.receiver.ReceiverRecord;

@SuppressWarnings("rawtypes")
public class ReceiverRecordException extends RuntimeException {

    private final ReceiverRecord record;

    ReceiverRecordException(ReceiverRecord record, Throwable t) {
        super(t);
        this.record = record;
    }

    public ReceiverRecord getRecord() {
        return this.record;
    }
}