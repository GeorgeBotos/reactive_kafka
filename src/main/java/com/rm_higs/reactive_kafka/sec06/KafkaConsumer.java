package com.rm_higs.reactive_kafka.sec06;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class KafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

	public static void start(String instanceId) {

		// The default Partition Assignment Strategy is RangeAssignor
		var consumerConfig = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
		                                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
		                                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId,
		                                            ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
		                                            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

		var options = ReceiverOptions.create(consumerConfig)
		                             .subscription(List.of("order-events"));
		KafkaReceiver.create(options)
		             .receive()
		             .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
		             .doOnNext(record -> record.receiverOffset()
		                                       .acknowledge())
		             .subscribe();
	}
}
