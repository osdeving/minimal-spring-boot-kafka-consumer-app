package com.nttdata.willams;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Log4j2
public class App {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = new Properties();

		properties.put("bootstrap.servers", "127.0.0.1:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("acks", "all");
		properties.put("retries", 1);
		properties.put("batch.size", 20000);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 24568545);


		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		Callback callback = (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
			}

			log.info("\n[Send OK] - " + "[topic = " + data.topic() + "] - " +
					"[partition = " + data.partition() + "] - " +
					"[offset = " + data.offset() + "] - " +
					"[timestamp = " + data.timestamp() + "]");
		};

		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> data = new ProducerRecord<>("teste1", "Hello this is record " + 1);
			producer.send(data, callback).get();
		}

        producer.close();
	}
}
