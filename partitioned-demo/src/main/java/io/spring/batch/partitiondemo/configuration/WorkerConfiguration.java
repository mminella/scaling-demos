/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.partitiondemo.configuration;

import javax.sql.DataSource;

import io.spring.batch.partitiondemo.domain.Transaction;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

@Configuration
@Profile("!master")
public class WorkerConfiguration {

	private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;


	public WorkerConfiguration(RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory) {
		this.workerStepBuilderFactory = workerStepBuilderFactory;
	}

	/*
	 * Configure inbound flow (requests coming from the master)
	 */
	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
		return IntegrationFlows
				.from(Amqp.inboundAdapter(connectionFactory, "requests"))
				.channel(requests())
				.get();
	}

	/*
	 * Configure the worker step
	 */
	@Bean
	public Step workerStep() {
		return this.workerStepBuilderFactory.get("workerStep")
				.inputChannel(requests())
				.<Transaction, Transaction>chunk(100)
				.reader(fileTransactionReader(null))
				.processor((ItemProcessor<Transaction, Transaction>) transaction -> {
					System.out.println("processing transaction = " + transaction);
					return transaction;
				})
				.writer(writer(null))
				.build();
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Transaction> fileTransactionReader(
			@Value("#{stepExecutionContext['file']}") Resource resource) {

		return new FlatFileItemReaderBuilder<Transaction>()
				.name("flatFileTransactionReader")
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
				.beanMapped()
				.sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
				.build();
	}

}
