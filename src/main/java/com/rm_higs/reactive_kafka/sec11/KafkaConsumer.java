package com.rm_higs.reactive_kafka.sec11;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(com.rm_higs.reactive_kafka.sec08.KafkaConsumer.class);

	public static void main(String[] args) {

		var consumerConfig = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
		                                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
		                                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
		                                            ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
		                                            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3); // default value is 500
		var options = ReceiverOptions.<String, String>create(consumerConfig)
		                             .commitInterval(Duration.ofSeconds(1))
		                             .subscription(List.of("order-events"));
		KafkaReceiver.create(options)
		             .receive()
		             .groupBy(record -> Integer.parseInt(record.key()) % 5)
		             // Grouping can be also done by record.partition(), i.e the partition number of the Kafka topic
		             // or by e.g. record.key().hashCode() % 5
		             .flatMap(KafkaConsumer::batchProcessRecords)
		             .subscribe();
	}

	private static Mono<Void> batchProcessRecords(GroupedFlux<Integer, ReceiverRecord<String, String>> flux) {
		return flux.publishOn(Schedulers.boundedElastic())
		           .doFirst(() -> log.info("---------------------mod: {}", flux.key()))
		           .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
				   .doOnNext(record -> record.receiverOffset()
				                             .acknowledge())
		           .then(Mono.delay(Duration.ofSeconds(1)))
		           .then();
	}
}
