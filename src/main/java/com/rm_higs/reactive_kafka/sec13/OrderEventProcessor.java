package com.rm_higs.reactive_kafka.sec13;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.concurrent.ThreadLocalRandom;

@RequiredArgsConstructor
public class OrderEventProcessor {

	private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);

	private final ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;

	public Mono<Void> process(ReceiverRecord<String, String> receiverRecord) {
		return Mono.just(receiverRecord)
		           .doOnNext(record -> {
//			           if (record.key().endsWith("5")) {
//				           throw new RuntimeException("processing exception");
//			           }

			           var index = ThreadLocalRandom.current()
			                                        .nextInt(1, 10);
			           log.info("key: {}, index: {}, value: {}",
			                    record.key(),
			                    index,
			                    record.value()
			                          .toString()
			                          .toCharArray()[index]);
			           record.receiverOffset()
			                 .acknowledge();
		           })
		           .onErrorMap(throwable -> new RecordProcessingException(receiverRecord, throwable))
				   .transform(deadLetterTopicProducer.recordProcessingErrorHandler());

	}
}
