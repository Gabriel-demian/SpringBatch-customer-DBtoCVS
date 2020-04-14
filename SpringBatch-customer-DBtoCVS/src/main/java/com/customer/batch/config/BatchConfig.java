package com.customer.batch.config;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

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

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

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
	
	
	Date now = new Date(); // java.util.Date, NOT java.sql.Date or java.sql.Timestamp!
	String format1 = new SimpleDateFormat("yyyy-MM-dd'-'HH-mm-ss-SSS", Locale.ENGLISH).format(now);
	private Resource outputResource = new FileSystemResource("output/customers_" + format1 + ".csv");
	
	
	
	@Bean
	public FlatFileItemWriter<Customer> writer(){
		
		//Create writer instance
		FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
		
		//Set output file location
		//writer.setResource(new ClassPathResource("customers.csv"));
		writer.setResource(outputResource);
		writer.setAppendAllowed(true);
		
		writer.setLineAggregator(new DelimitedLineAggregator<Customer>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Customer>() {
                    {
                        setNames(new String[] {"id","firstName","lastName","email"});
                    }
                });
            }
        });
		
		/*
		DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<Customer>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Customer>  fieldExtractor = new BeanWrapperFieldExtractor<Customer>();
		fieldExtractor.setNames(new String[]{"id","firstName","lastName","email"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		*/
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

