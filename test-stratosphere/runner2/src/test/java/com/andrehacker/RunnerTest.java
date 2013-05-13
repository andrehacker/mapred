package com.andrehacker;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RunnerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		Runner runner = new Runner();
		Runner.main(new String[] {"job-wordcount.json"});
	}

}
