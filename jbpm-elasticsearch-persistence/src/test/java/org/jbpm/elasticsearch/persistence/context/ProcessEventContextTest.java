package org.jbpm.elasticsearch.persistence.context;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test for {@link ProcessEventContext}.
 * <p/>
 * We will not test the getters and setters of {@link ProcessEventContext}
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class ProcessEventContextTest {

	@Test
	public void testChangeVariable() {
		ProcessEventContext context = new ProcessEventContext("org.jbpm:test-context:1.0.0", "testProcessId", 1L);
		String testKey = "testKey";
		String firstValue = "firstValue";
		context.changeVariable(testKey, firstValue);
		assertEquals("firstValue", context.getChangedVariables().get(testKey));
		
		String  secondValue = "secondValue";
		context.changeVariable(testKey, secondValue);
		assertEquals("secondValue", context.getChangedVariables().get(testKey));
	}
		
}
