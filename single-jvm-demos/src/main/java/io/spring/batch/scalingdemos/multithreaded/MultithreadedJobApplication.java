/**
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.scalingdemos.multithreaded;

import javax.sql.DataSource;

import io.spring.batch.scalingdemos.domain.Transaction;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @author Michael Minella
 */
@EnableBatchProcessing
@SpringBootApplication
public class MultithreadedJobApplication {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	@StepScope
	public FlatFileItemReader<Transaction> fileTransactionReader(
			@Value("#{jobParameters['inputFlatFile']}") Resource resource) {

		return new FlatFileItemReaderBuilder<Transaction>()
				.name("transactionItemReader")
				.resource(resource)
				.delimited()
				.names(new String[] {"account", "amount", "timestamp"})
				.fieldSetMapper(fieldSet -> {
					Transaction transaction = new Transaction();

					transaction.setAccount(fieldSet.readString("account"));
					transaction.setAmount(fieldSet.readBigDecimal("amount"));
					transaction.setTimestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"));

					return transaction;
				})
				.build();
	}

	@Bean
	@StepScope
	public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
				.beanMapped()
				.build();
	}

	@Bean
	public Job multithreadedJob() {
		return this.jobBuilderFactory.get("multithreadedJob")
				.start(step1())
				.build();
	}

	@Bean
	public Step step1() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(4);
		taskExecutor.setMaxPoolSize(4);
		taskExecutor.afterPropertiesSet();

		return this.stepBuilderFactory.get("step1")
				.<Transaction, Transaction>chunk(100)
				.reader(fileTransactionReader(null))
				.writer(writer(null))
				.taskExecutor(taskExecutor)
				.build();
	}

	public static void main(String[] args) {
		String [] newArgs = new String[] {"inputFlatFile=/data/csv/bigtransactions.csv"};

		SpringApplication.run(MultithreadedJobApplication.class, newArgs);
	}
}
