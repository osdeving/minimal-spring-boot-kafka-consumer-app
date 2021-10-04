package com.nttdata.willams.kafka.minimal.consumer;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
@Log4j2
public class Application {
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("topico.exemplo").partitions(10).replicas(1).build();
	}

	@KafkaListener(id = "myId", topics = "topico.exemplo")
	public void listen(String in) {
		log.info(in);
	}
}
