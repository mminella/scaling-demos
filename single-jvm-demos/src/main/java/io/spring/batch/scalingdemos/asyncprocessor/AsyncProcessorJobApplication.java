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
package io.spring.batch.scalingdemos.asyncprocessor;

import javax.sql.DataSource;

import io.spring.batch.scalingdemos.domain.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.StopWatch;

/**
 * @author Michael Minella
 */
@EnableBatchProcessing
@SpringBootApplication
@SuppressWarnings("unchecked")
public class AsyncProcessorJobApplication {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

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
	@StepScope
	public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
				.build();
	}

	@Bean
	public ItemProcessor<Transaction, Transaction> processor() {
		return (transaction) -> {
			Thread.sleep(5);
			return transaction;
		};
	}

	@Bean
	public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor() {
		AsyncItemProcessor<Transaction, Transaction> processor = new AsyncItemProcessor<>();

		processor.setDelegate(processor());
		processor.setTaskExecutor(new SimpleAsyncTaskExecutor());

		return processor;
	}

	@Bean
	public AsyncItemWriter<Transaction> asyncItemWriter() {
		AsyncItemWriter<Transaction> writer = new AsyncItemWriter<>();

		writer.setDelegate(writer(null));

		return writer;
	}

	@Bean
	public Job asyncJob() {
		return this.jobBuilderFactory.get("asyncJob")
				.start(step1async())
				.listener(new ExecutionTimeJobListener())
				.build();
	}

	@Bean
	public Step step1async() {
		return this.stepBuilderFactory.get("step1async")
				.<Transaction, Transaction>chunk(100)
				.reader(fileTransactionReader(null))
				.processor((ItemProcessor) asyncItemProcessor())
				.writer(asyncItemWriter())
				.build();
	}

	class ExecutionTimeJobListener implements JobExecutionListener {

		private Logger logger = LoggerFactory.getLogger(ExecutionTimeJobListener.class);
		private StopWatch stopWatch = new StopWatch();

		@Override
		public void beforeJob(JobExecution jobExecution) {
			stopWatch.start();
		}

		@Override
		public void afterJob(JobExecution jobExecution) {
			stopWatch.stop();
			logger.info("Job took " + stopWatch.getTotalTimeSeconds() + "s");
		}
	}

	public static void main(String[] args) {
		String [] newArgs = new String[] {"inputFlatFile=/data/csv/bigtransactions.csv"};

		SpringApplication.run(AsyncProcessorJobApplication.class, newArgs);
	}
}
