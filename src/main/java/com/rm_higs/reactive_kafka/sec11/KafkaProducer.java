package com.rm_higs.reactive_kafka.sec11;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class KafkaProducer {

	public static final Logger log = LoggerFactory.getLogger(com.rm_higs.reactive_kafka.sec08.KafkaProducer.class);
	public static void main(String[] args) {
		var producerConfig = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
		                                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
		                                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		var options = SenderOptions.<String, String>create(producerConfig);
		var inputFlux = Flux.range(1, 100)
		                    .map(index -> new ProducerRecord<>("order-events",
		                                                       index.toString(),
		                                                       "order-" + index))
		                    .map(producerRecord -> SenderRecord.create(producerRecord, producerRecord.key()));

		var sender = KafkaSender.create(options);
		sender.send(inputFlux)
		      .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata()))
		      .doOnComplete(sender::close)
		      .subscribe();
	}
}
