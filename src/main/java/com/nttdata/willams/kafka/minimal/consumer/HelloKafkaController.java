package com.nttdata.willams.kafka.minimal.consumer;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@RestController
@Log4j2
public class HelloKafkaController {
	

	private final KafkaTemplate<String, Object> template;
	private final String topicName;
	private final int messagesPerRequest;
	private CountDownLatch latch;

	public HelloKafkaController(
			final KafkaTemplate<String, Object> template,
			@Value("${tpd.topic-name}") final String topicName,
			@Value("${tpd.messages-per-request}") final int messagesPerRequest) {
		this.template = template;
		this.topicName = topicName;
		this.messagesPerRequest = messagesPerRequest;
	}

	@GetMapping("/hello")
	public String hello() throws Exception {
		latch = new CountDownLatch(messagesPerRequest);
		IntStream.range(0, messagesPerRequest)
				.forEach(i -> this.template.send(topicName, String.valueOf(i),
						new PracticalAdvice("A Practical Advice", i))
				);
		latch.await(60, TimeUnit.SECONDS);
		log.info("All messages received");
		return "Hello Kafka!";
	}

	@KafkaListener(topics = "advice-topic", clientIdPrefix = "json",
			containerFactory = "kafkaListenerContainerFactory")
	public void listenAsObject(ConsumerRecord<String, PracticalAdvice> cr,
	                           @Payload PracticalAdvice payload) {
		log.info("log 1 [JSON] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
				typeIdHeader(cr.headers()), payload, cr.toString());
		latch.countDown();
	}

	@KafkaListener(topics = "advice-topic", clientIdPrefix = "string",
			containerFactory = "kafkaListenerStringContainerFactory")
	public void listenasString(ConsumerRecord<String, String> cr,
	                           @Payload String payload) {
		log.info("log 2 [String] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
				typeIdHeader(cr.headers()), payload, cr.toString());
		latch.countDown();
	}

	@KafkaListener(topics = "advice-topic", clientIdPrefix = "bytearray",
			containerFactory = "kafkaListenerByteArrayContainerFactory")
	public void listenAsByteArray(ConsumerRecord<String, byte[]> cr,
	                              @Payload byte[] payload) {
		log.info("log 3 [ByteArray] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
				typeIdHeader(cr.headers()), payload, cr.toString());
		latch.countDown();
	}

	private static String typeIdHeader(Headers headers) {
		return StreamSupport.stream(headers.spliterator(), false)
				.filter(header -> header.key().equals("__TypeId__"))
				.findFirst().map(header -> new String(header.value())).orElse("N/A");
	}
}
