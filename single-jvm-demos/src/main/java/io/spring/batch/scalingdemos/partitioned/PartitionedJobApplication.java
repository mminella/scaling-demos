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
package io.spring.batch.scalingdemos.partitioned;

import javax.sql.DataSource;

import io.spring.batch.scalingdemos.domain.Transaction;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
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

/**
 * @author Michael Minella
 */
@EnableBatchProcessing
@SpringBootApplication
public class PartitionedJobApplication {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

//	@Bean
//	public Job parallelStepsJob() {
//	}
//
//	@Bean
//	public Job sequentialStepsJob() {
//	}

	@Bean
	public MultiResourcePartitioner partitioner(@Value("#{jobParameters['inputFiles']}") Resource [] resources) {
		MultiResourcePartitioner partitioner = new MultiResourcePartitioner();

		partitioner.setKeyName("file");
		partitioner.setResources(resources);

		return partitioner;
	}
//
//	@Bean
//	public PartitionHandler partitionHandler(TaskLauncher taskLauncher, JobExplorer jobExplorer) throws Exception {
//		Resource resource = resourceLoader.getResource("maven://io.spring.cloud:partitioned-batch-job:1.1.0.RELEASE");
//
//		DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "workerStep");
//
//		List<String> commandLineArgs = new ArrayList<>(3);
//		commandLineArgs.add("--spring.profiles.active=worker");
//		commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
//		commandLineArgs.add("--spring.batch.initializer.enabled=false");
//		partitionHandler.setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
//		partitionHandler.setEnvironmentVariablesProvider(new SimpleEnvironmentVariablesProvider(this.environment));
//		partitionHandler.setMaxWorkers(1);
//		partitionHandler.setApplicationName("PartitionedBatchJobTask");
//
//		return partitionHandler;
//	}

	@Bean
	@StepScope
	public FlatFileItemReader<Transaction> fileTransactionReader(
			@Value("#{stepExecutionContext['inputFlatFile']}") Resource resource) {

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

	@Bean
	public Step step1() {
		return this.stepBuilderFactory.get("step1")
				.<Transaction, Transaction>chunk(100)
				.reader(fileTransactionReader(null))
				.writer(writer(null))
				.build();
	}

//	@Bean
//	public Step partitionedMaster() {
//		return this.stepBuilderFactory.get("step1")
//				.partitioner(step1().getName(), partitioner(null))
//				.step(step1())
//				.partitionHandler(partitionHandler)
//				.build();
//	}

	public static void main(String[] args) {
		String [] newArgs = new String[] {"inputFiles=/data/transactions*.csv"};

		SpringApplication.run(PartitionedJobApplication.class, newArgs);
	}
}
