package io.spring.batch.remotechunking;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class RemoteChunkingApplication {

	public static void main(String[] args) {
		SpringApplication.run(RemoteChunkingApplication.class, args);
	}
}
