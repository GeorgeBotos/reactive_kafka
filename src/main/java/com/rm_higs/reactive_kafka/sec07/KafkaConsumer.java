package com.rm_higs.reactive_kafka.sec07;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class KafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

	public static void main(String[] args) {

		var consumerConfig = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
		                                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
		                                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
													ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
													ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
		                                            ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123");
		var options = ReceiverOptions.create(consumerConfig)
		                             .addAssignListener(partitions -> {
			                             partitions.forEach(partition -> log.info("Assigned {}", partition.position()));
//										 collection.forEach(record -> record.seek(record.position()-2));
			                             partitions.stream()
			                                       .filter(partition -> partition.topicPartition()
			                                                                     .partition() == 2)
			                                       .findFirst()
			                                       .ifPresent(partition -> partition.seek(partition.position() - 10));
			                             // Other options are partition.seekToBeginning()
			                             //                   partition.seekToEnd()
			                             //                   partition.seekTimestamp()
		                             })
		                             .subscription(List.of("order-events"));
		KafkaReceiver.create(options)
		             .receive()
		             .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
		             .doOnNext(record -> record.receiverOffset()
		                                       .acknowledge())
		             .subscribe();
	}
}
