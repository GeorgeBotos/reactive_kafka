package com.rm_higs.reactive_kafka.sec12;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaConsumerV3 {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerV3.class);

	public static void main(String[] args) {

		var consumerConfig = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
		                                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
		                                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
		                                            ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123");
		var options = ReceiverOptions.create(consumerConfig)
		                             .subscription(List.of("order-events"));
		KafkaReceiver.create(options)
		             .receive()
		             .log()
		             .concatMap(KafkaConsumerV3::processRecord)
		             .subscribe();
	}

	private static Mono<Void> processRecord(ReceiverRecord<Object, Object> receiverRecord) {
		return Mono.just(receiverRecord)
		           .doOnNext(record -> {
			           if (record.key().toString().equals("5")) {
				           throw new RuntimeException("DB is down");
			           }
			           var index = ThreadLocalRandom.current()
			                                        .nextInt(1, 20);
			           log.info("Key: {}, index: {}, value: {}",
			                    record.key(),
			                    index,
			                    record.value()
			                          .toString()
			                          .toCharArray()[index]);
			           record.receiverOffset()
			                 .acknowledge();
		           })
		           .retryWhen(getRetry())
		           .doOnError(exception -> log.error(exception.getMessage()))
		           .onErrorResume(IndexOutOfBoundsException.class,
		                          exception -> Mono.fromRunnable(() -> receiverRecord.receiverOffset()
		                                                                             .acknowledge()))
		           .then();
	}

	private static Retry getRetry() {
		return Retry.fixedDelay(3, Duration.ofSeconds(1))
		            .filter(IndexOutOfBoundsException.class::isInstance)
		            .onRetryExhaustedThrow((spec, signal) -> signal.failure());
	}
}
