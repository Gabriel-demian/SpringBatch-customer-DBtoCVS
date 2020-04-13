package com.customer.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.customer.batch.model.Customer;

public class CustomerItemProcessor implements ItemProcessor<Customer, Customer>{

	@Override
	public Customer process(Customer customer) throws Exception {
		return null;
	}
	
	
	
}
