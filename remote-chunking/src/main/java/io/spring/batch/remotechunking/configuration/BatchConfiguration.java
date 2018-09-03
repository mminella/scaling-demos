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
package io.spring.batch.remotechunking.configuration;

import javax.sql.DataSource;

import io.spring.batch.remotechunking.domain.Transaction;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingMasterStepBuilderFactory;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

/**
 * @author Michael Minella
 */
@Configuration
public class BatchConfiguration {

	@Configuration
	@Profile("!worker")
	public static class MasterConfiguration {

		@Autowired
		private JobBuilderFactory jobBuilderFactory;

		@Autowired
		private RemoteChunkingMasterStepBuilderFactory remoteChunkingMasterStepBuilderFactory;

		@Bean
		public DirectChannel requests() {
			return new DirectChannel();
		}

		@Bean
		public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
			return IntegrationFlows.from("requests")
					.handle(Amqp.outboundAdapter(amqpTemplate)
							.routingKey("requests"))
					.get();
		}

		@Bean
		public QueueChannel replies() {
			return new QueueChannel();
		}

		@Bean
		public IntegrationFlow replyFlow(ConnectionFactory connectionFactory) {
			return IntegrationFlows
					.from(Amqp.inboundAdapter(connectionFactory, "replies"))
					.channel(replies())
					.get();
		}

		@Bean
		@StepScope
		public FlatFileItemReader<Transaction> fileTransactionReader(
				@Value("#{jobParameters['inputFlatFile']}") Resource resource) {

			return new FlatFileItemReaderBuilder<Transaction>()
					.saveState(false)
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
		public TaskletStep step1() {
			return this.remoteChunkingMasterStepBuilderFactory.get("step1")
					.<Transaction, Transaction>chunk(100)
					.reader(fileTransactionReader(null))
					.outputChannel(requests())
					.inputChannel(replies())
					.build();
		}

		@Bean
		public Job remoteChunkingJob() {
			return this.jobBuilderFactory.get("remoteChunkingJob")
					.start(step1())
					.build();
		}
	}

	@Configuration
	@Profile("worker")
	public static class WorkerConfiguration {

		@Autowired
		private RemoteChunkingWorkerBuilder<Transaction, Transaction> workerBuilder;

		@Bean
		public Queue requestQueue() {
			return new Queue("requests", false);
		}

		@Bean
		public Queue repliesQueue() {
			return new Queue("replies", false);
		}

		@Bean
		public TopicExchange exchange() {
			return new TopicExchange("remote-chunking-exchange");
		}

		@Bean
		Binding repliesBinding(TopicExchange exchange) {
			return BindingBuilder.bind(repliesQueue()).to(exchange).with("replies");
		}

		@Bean
		Binding requestBinding(TopicExchange exchange) {
			return BindingBuilder.bind(requestQueue()).to(exchange).with("requests");
		}

		@Bean
		public DirectChannel requests() {
			return new DirectChannel();
		}

		@Bean
		public DirectChannel replies() {
			return new DirectChannel();
		}

		@Bean
		public IntegrationFlow mesagesIn(ConnectionFactory connectionFactory) {
			return IntegrationFlows
					.from(Amqp.inboundAdapter(connectionFactory, "requests"))
					.channel(requests())
					.get();
		}

		@Bean
		public IntegrationFlow outgoingReplies(AmqpTemplate template) {
			return IntegrationFlows.from("replies")
					.handle(Amqp.outboundAdapter(template)
							.routingKey("replies"))
					.get();
		}

		@Bean
		public IntegrationFlow integrationFlow() {
			return this.workerBuilder
					.itemProcessor(itemProcessor())
					.itemWriter(writer(null))
					.inputChannel(requests())
					.outputChannel(replies())
					.build();
		}

		@Bean
		public ItemProcessor<Transaction, Transaction> itemProcessor() {
			return transaction -> {
				System.out.println("processing transaction = " + transaction);
				return transaction;
			};
		}

		@Bean
		public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
			return new JdbcBatchItemWriterBuilder<Transaction>()
					.dataSource(dataSource)
					.beanMapped()
					.sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
					.build();
		}
	}
}
