package com.example.batch;

import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.DecoratingProxy;

import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.FileCopyUtils;
import org.springframework.validation.BindException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;

@SpringBootApplication
@ImportRuntimeHints(BatchApplication.Hints.class)
public class BatchApplication {

	static class Hints  implements RuntimeHintsRegistrar{
		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader){
			hints.proxies().registerJdkProxy(JobOperator.class, SpringProxy.class, Advised.class, DecoratingProxy.class);
		}

	}

	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

	@Bean
	Job job(JobRepository jobRepository, @Qualifier("step1") Step step, @Qualifier("csvToDb") Step csvTobDb){
		return new JobBuilder("job",jobRepository)
				.start(step)
				.next(csvTobDb)
				.build();
	}

	@Bean
	ApplicationRunner runner(JobLauncher jobLauncher, Job job){
		return args -> {
			var jobParameters = new JobParametersBuilder()
					// .addString("uuid", UUID.randomUUID().toString())
					.addDate("date", new Date())
					.toJobParameters();
			var run = jobLauncher.run(job,jobParameters);
			var instanceId = run.getJobInstance().getInstanceId();
			System.out.println("InstanceId : "+ instanceId);
		};

	}

	@Bean
	@StepScope
	Tasklet tasklet(@Value("#{jobParameters['date']}") Date date){
		return (contribution, chunkContext) -> {
			System.out.println("Hello world the uuid is: " + date);
			return RepeatStatus.FINISHED;
		};
	}

	record CsvRow(int rank, String name, String platform, int year, String genre, String publisher, float na, float eu, float other, float global,
				  float v){}

	@Bean
	FlatFileItemReader <CsvRow> csvRowFlatFileItemReader (Resource resource){
		return new FlatFileItemReaderBuilder<CsvRow>()
				.resource(resource)
				.delimited().delimiter(",")
				.names("rank,name,platform,year,genre,publisher,na,eu,jp,other,global".split(","))
				.linesToSkip(1)
				.fieldSetMapper(fieldSet -> new CsvRow(
                        fieldSet.readInt(0),
                        fieldSet.readString(1),
                        fieldSet.readString(2),
                        fieldSet.readInt(3),
                        fieldSet.readString(4),
                        fieldSet.readString(5),
                        fieldSet.readFloat(6),
                        fieldSet.readFloat(7),
                        fieldSet.readFloat(8),
                        fieldSet.readFloat(9),
                        fieldSet.readFloat(10)
                ))
				.build();
	}

	@Bean
    Step csvToDb(JobRepository repository, PlatformTransactionManager txm,
				 @Value("file:///D:/learning/spring-batch-learning/batch/data/vgsales.csv") Resource data) throws IOException {

		String[] lines;
		try (var reader = new InputStreamReader(data.getInputStream())){
			var string = FileCopyUtils.copyToString(reader);
			lines = string.split(System.lineSeparator());
			System.out.println("There are "+ lines.length+ " rows");
		}

		return new StepBuilder("csvToDb", repository)
				.chunk(100,txm)
				.reader(new ListItemReader<Object>(Arrays.asList(lines)))
				.writer(new ItemWriter<Object>() {
					@Override
					public void write(Chunk<?> chunk) throws Exception {
						var oneHundredRows = chunk.getItems();
						System.out.println(oneHundredRows);
					}
				})
				.build();
	}

	@Bean
	JdbcTemplate jdbcTemplate(DataSource dataSource){
		return new JdbcTemplate(dataSource);
	}

	@Bean
	Step step1(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager transactionManager){
		return new StepBuilder("step1",jobRepository).tasklet(tasklet,transactionManager).build();
	}

}
