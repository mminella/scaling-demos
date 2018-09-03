package io.spring.batch.partitiondemo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@EnableBatchIntegration
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		List<String> strings = Arrays.asList(args);

		List<String> finalArgs = new ArrayList<>(strings.size() + 1);
		finalArgs.addAll(strings);
		finalArgs.add("inputFiles=/data/csv/transactions*.csv");

		SpringApplication.run(DemoApplication.class, finalArgs.toArray(new String[finalArgs.size()]));
	}
}
