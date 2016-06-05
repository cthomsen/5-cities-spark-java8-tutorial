package de.bst.five_cities.utils.test;

import static java.lang.System.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class AbstractTest {
	@Rule
	public TestName testName = new TestName();

	@Before
	public void setUp() {
		out.println("## Performing test " + testName.getMethodName());
		out.println();
	}

	@After
	public void tearDown() {
		out.println();
		out.println("Finished " + testName.getMethodName());
		out.println();
	}

}
