package com.cbcalla.kafkaconsumerdemo.exception;

import reactor.kafka.receiver.ReceiverRecord;

@SuppressWarnings("rawtypes")
public class ReceiverRecordException extends RuntimeException {

    private final ReceiverRecord record;

    public ReceiverRecordException(ReceiverRecord record, Throwable t) {
        super(t);
        this.record = record;
    }

    public ReceiverRecord getRecord() {
        return this.record;
    }
}