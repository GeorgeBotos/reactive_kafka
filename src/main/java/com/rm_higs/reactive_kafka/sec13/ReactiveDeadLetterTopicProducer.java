package com.rm_higs.reactive_kafka.sec13;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

@RequiredArgsConstructor
public class ReactiveDeadLetterTopicProducer<K, V> {

	private static final Logger log = LoggerFactory.getLogger(ReactiveDeadLetterTopicProducer.class);
	private final KafkaSender<K, V> sender;
	private final Retry retrySpec;

	public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> receiverRecord) {
		var senderRecord = toSenderRecord(receiverRecord);
		return sender.send(Mono.just(senderRecord))
		             .next();
	}

	public SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> receiverRecord) {
		var producerRecord = new ProducerRecord<>(receiverRecord.topic() + "-dlt", receiverRecord.key(), receiverRecord.value());
		return SenderRecord.create(producerRecord, producerRecord.key());
	}

	public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
		return mono -> mono.retryWhen(retrySpec)
		                   .onErrorMap(exception -> exception.getCause() instanceof RecordProcessingException, Throwable::getCause)
		                   .doOnError(exception -> log.error(exception.getMessage()))
		                   .onErrorResume(RecordProcessingException.class,
		                                  exception -> produce(exception.getReceiverRecord()).then(Mono.fromRunnable(() -> exception.getReceiverRecord()
		                                                                                                                            .receiverOffset()
		                                                                                                                            .acknowledge())))
		                   .then();
	}
}
