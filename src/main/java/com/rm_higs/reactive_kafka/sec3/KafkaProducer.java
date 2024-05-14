package com.rm_higs.reactive_kafka.sec3;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaProducer {

	public static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
	public static void main(String[] args) {
		var producerConfig = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
		                                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
		                                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		var options = SenderOptions.<String, String>create(producerConfig)
		                           .maxInFlight(10_000);
		var inputFlux = Flux.range(1, 1_000_000)
		                    .map(index -> new ProducerRecord<>("order-events",
		                                                       index.toString(),
		                                                       "order-" + index))
		                    .map(producerRecord -> SenderRecord.create(producerRecord, producerRecord.key()));
		var start = System.currentTimeMillis();
		var sender = KafkaSender.create(options);
		sender.send(inputFlux)
		      .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata()))
		      .doOnComplete(() -> {
				  log.info("Total time taken: {}ms", (System.currentTimeMillis()-start));
				  sender.close();
			  })
		      .subscribe();

	}
}
