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

import java.util.concurrent.Future;

import javax.sql.DataSource;

import io.spring.batch.scalingdemos.domain.Transaction;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author Michael Minella
 */
@SpringBootApplication
public class AsyncProcessorJobApplication {

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
	public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
				.build();
	}

	@Bean
	public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor() {
		AsyncItemProcessor<Transaction, Transaction> processor = new AsyncItemProcessor<>();

		processor.setDelegate(processor());
		processor.setTaskExecutor(new VirtualThreadTaskExecutor());

		return processor;
	}

	@Bean
	public AsyncItemWriter<Transaction> asyncItemWriter() {
		AsyncItemWriter<Transaction> writer = new AsyncItemWriter<>();

		writer.setDelegate(writer(null));

		return writer;
	}


	// Adds 5 minutes to the processing of the job where the file is 60000 records long
	@Bean
	public ItemProcessor<Transaction, Transaction> processor() {
		return (transaction) -> {
			Thread.sleep(5);
			return transaction;
		};
	}

	@Bean
	public Job asyncJob(JobRepository jobRepository) {
		return new JobBuilder("asyncJob", jobRepository)
				.start(step1async(null, null))
				.build();
	}

//	@Bean
//	public Job job1(JobRepository jobRepository) {
//		return new JobBuilder("job1", jobRepository)
//				.start(step1(null, null))
//				.build();
//	}

	@Bean
	public Step step1async(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("step1async", jobRepository)
				.<Transaction, Future<Transaction>>chunk(100, transactionManager)
				.reader(fileTransactionReader(null))
				.processor(asyncItemProcessor())
				.writer(asyncItemWriter())
				.build();
	}

//	@Bean
//	public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
//		return new StepBuilder("step1", jobRepository)
//				.<Transaction, Transaction>chunk(100, transactionManager)
//				.reader(fileTransactionReader(null))
//				.processor(processor())
//				.writer(writer(null))
//				.build();
//	}

	public static void main(String[] args) {
		String [] newArgs = new String[] {"inputFlatFile=/data/csv/bigtransactions.csv"};

		SpringApplication.run(AsyncProcessorJobApplication.class, newArgs);
	}
}
