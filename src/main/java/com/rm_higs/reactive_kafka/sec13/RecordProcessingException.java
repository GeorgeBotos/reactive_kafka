package com.rm_higs.reactive_kafka.sec13;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {

	private final ReceiverRecord<?, ?> receiverRecord;

	public RecordProcessingException(ReceiverRecord<?, ?> receiverRecord, Throwable exception) {
		super(exception);
		this.receiverRecord = receiverRecord;
	}

	@SuppressWarnings("unchecked")
	public <K, V> ReceiverRecord<K, V> getReceiverRecord() {
		return (ReceiverRecord<K, V>) receiverRecord;
	}
}
