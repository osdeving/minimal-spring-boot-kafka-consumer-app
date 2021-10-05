package com.nttdata.willams.kafka.minimal.consumer;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@Log4j2
public class Application {
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	private KafkaProperties kafkaProperties;

	@Value("${tpd.topic-name}")
	private String topicName;

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return props;
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name(topicName).partitions(10).replicas(1).build();
	}

	@Bean
	public ProducerFactory<String, Object> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ConsumerFactory<String, Object> consumerFactory() {
		final JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
		jsonDeserializer.addTrustedPackages("*");
		return new DefaultKafkaConsumerFactory<>(
				kafkaProperties.buildConsumerProperties(), new StringDeserializer(), jsonDeserializer
		);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());

		return factory;
	}

	@Bean
	public ConsumerFactory<String, String> stringConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(
				kafkaProperties.buildConsumerProperties(), new StringDeserializer(), new StringDeserializer()
		);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerStringContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(stringConsumerFactory());

		return factory;
	}

	@Bean
	public ConsumerFactory<String, byte[]> byteArrayConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(
				kafkaProperties.buildConsumerProperties(), new StringDeserializer(), new ByteArrayDeserializer()
		);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerByteArrayContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(byteArrayConsumerFactory());
		return factory;
	}

}
