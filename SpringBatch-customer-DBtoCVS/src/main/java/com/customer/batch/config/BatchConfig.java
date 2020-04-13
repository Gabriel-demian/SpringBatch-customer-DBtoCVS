package com.customer.batch.config;


import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.customer.batch.model.Customer;
import com.customer.batch.processor.CustomerItemProcessor;


@Configuration
@EnableBatchProcessing
public class BatchConfig {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public JdbcCursorItemReader<Customer> reader(){
		JdbcCursorItemReader<Customer> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql("SELECT id, first_name, last_name, email FROM customer");
		cursorItemReader.setRowMapper(new CustomerRowMapper());
		
		return cursorItemReader;
	}
	
	
	@Bean
	public CustomerItemProcessor processor(){
		return new CustomerItemProcessor();
	}
	
	@Bean
	public FlatFileItemWriter<Customer> writer(){
		FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
		writer.setResource(new ClassPathResource("customers.csv"));
		
		DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<Customer>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Customer>  fieldExtractor = new BeanWrapperFieldExtractor<Customer>();
		fieldExtractor.setNames(new String[]{"id","first_name","last_name","email"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		return writer;
	}
	
	@Bean
	public Step step1(){
		return stepBuilderFactory.get("step1").<Customer,Customer>chunk(100).reader(reader()).processor(processor()).writer(writer()).build();
	}
	
	@Bean
	public Job exportPerosnJob(){
		return jobBuilderFactory.get("exportPeronJob").incrementer(new RunIdIncrementer()).flow(step1()).end().build();
	}
	
	
}

