package org.jbpm.elasticsearch.persistence.listener;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;

import org.drools.persistence.TransactionManager;
import org.jbpm.elasticsearch.persistence.context.ProcessEventContext;
import org.jbpm.elasticsearch.persistence.context.ProcessEventContext.ProcessState;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.event.process.ProcessCompletedEvent;
import org.kie.api.event.process.ProcessEventListener;
import org.kie.api.event.process.ProcessStartedEvent;
import org.kie.api.event.process.ProcessVariableChangedEvent;
import org.kie.api.runtime.Environment;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.KieRuntime;
import org.kie.api.runtime.process.ProcessInstance;
import org.mockito.Mockito;

public class ElasticSeachProcessEventListenerTest {

	private ProcessEventListener listener;

	@Before
	public void before() {
		String elasticSearchEndpointUrl = "http://localhost:9800";
		listener = new ElasticSearchProcessEventListener(elasticSearchEndpointUrl);
	}

	/**
	 * Test that the process-state on the context is set to "STARTING".
	 * 
	 */
	@Test
	public void testAfterProcessStarted() throws Exception {
		// We need to mock this stuff.
		ProcessStartedEvent event = Mockito.mock(ProcessStartedEvent.class);
		ProcessInstance pInstance = Mockito.mock(ProcessInstance.class);
		Mockito.when(event.getProcessInstance()).thenReturn(pInstance);
		Mockito.when(pInstance.getProcessId()).thenReturn("testProcessId");
		Mockito.when(pInstance.getState()).thenReturn(ProcessInstance.STATE_ACTIVE);

		// Mock the KieRuntime and the environment.
		KieRuntime kieRuntime = Mockito.mock(KieRuntime.class);
		Environment environment = Mockito.mock(Environment.class);
		Mockito.when(event.getKieRuntime()).thenReturn(kieRuntime);
		Mockito.when(kieRuntime.getEnvironment()).thenReturn(environment);
		Mockito.when(environment.get("deploymentId")).thenReturn("testDeploymentId");

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(environment.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		listener.afterProcessStarted(event);

		// Retrieve the context.
		Field threadLocalProcessContextField = listener.getClass().getDeclaredField("processContext");
		threadLocalProcessContextField.setAccessible(true);
		ThreadLocal<ProcessEventContext> threadLocalProcessContext = (ThreadLocal<ProcessEventContext>) threadLocalProcessContextField
				.get(listener);
		ProcessEventContext processContext = threadLocalProcessContext.get();

		// Assert that the correct IDs have been set on the context.
		assertEquals("testProcessId", processContext.getProcessId());
		assertEquals("testDeploymentId", processContext.getDeploymentUnit());

		// Assert the process state.
		assertEquals("ACTIVE", processContext.getProcessState());
		assertEquals(ProcessState.STARTING, processContext.getIndexingProcessState());

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
	}

	@Test
	public void testAfterProcessCompleted() throws Exception {
		// We need to mock this stuff.
		ProcessCompletedEvent event = Mockito.mock(ProcessCompletedEvent.class);
		ProcessInstance pInstance = Mockito.mock(ProcessInstance.class);
		Mockito.when(event.getProcessInstance()).thenReturn(pInstance);
		Mockito.when(pInstance.getProcessId()).thenReturn("testProcessId");
		Mockito.when(pInstance.getState()).thenReturn(ProcessInstance.STATE_COMPLETED);

		// Mock the KieRuntime and the environment.
		KieRuntime kieRuntime = Mockito.mock(KieRuntime.class);
		Environment environment = Mockito.mock(Environment.class);
		Mockito.when(event.getKieRuntime()).thenReturn(kieRuntime);
		Mockito.when(kieRuntime.getEnvironment()).thenReturn(environment);
		Mockito.when(environment.get("deploymentId")).thenReturn("testDeploymentId");

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(environment.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		listener.afterProcessCompleted(event);

		// Retrieve the context.
		Field threadLocalProcessContextField = listener.getClass().getDeclaredField("processContext");
		threadLocalProcessContextField.setAccessible(true);
		ThreadLocal<ProcessEventContext> threadLocalProcessContext = (ThreadLocal<ProcessEventContext>) threadLocalProcessContextField
				.get(listener);
		ProcessEventContext processContext = threadLocalProcessContext.get();

		// Assert that the correct IDs have been set on the context.
		assertEquals("testProcessId", processContext.getProcessId());
		assertEquals("testDeploymentId", processContext.getDeploymentUnit());

		// Assert the process state.
		assertEquals("COMPLETED", processContext.getProcessState());
		assertEquals(ProcessState.COMPLETING, processContext.getIndexingProcessState());

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
	}

	@Test
	public void testAfterVariableChanged() throws Exception {
		// We need to mock this stuff.
		ProcessVariableChangedEvent event = Mockito.mock(ProcessVariableChangedEvent.class);
		ProcessInstance pInstance = Mockito.mock(ProcessInstance.class);
		Mockito.when(event.getProcessInstance()).thenReturn(pInstance);
		Mockito.when(pInstance.getProcessId()).thenReturn("testProcessId");
		Mockito.when(pInstance.getState()).thenReturn(ProcessInstance.STATE_COMPLETED);

		// Mock the variable change.
		Mockito.when(event.getVariableId()).thenReturn("testVariableId");
		Mockito.when(event.getNewValue()).thenReturn("newTestValue");

		// Mock the KieRuntime and the environment.
		KieRuntime kieRuntime = Mockito.mock(KieRuntime.class);
		Environment environment = Mockito.mock(Environment.class);
		Mockito.when(event.getKieRuntime()).thenReturn(kieRuntime);
		Mockito.when(kieRuntime.getEnvironment()).thenReturn(environment);
		Mockito.when(environment.get("deploymentId")).thenReturn("testDeploymentId");

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(environment.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		listener.afterVariableChanged(event);

		// Retrieve the context.
		Field threadLocalProcessContextField = listener.getClass().getDeclaredField("processContext");
		threadLocalProcessContextField.setAccessible(true);
		ThreadLocal<ProcessEventContext> threadLocalProcessContext = (ThreadLocal<ProcessEventContext>) threadLocalProcessContextField
				.get(listener);
		ProcessEventContext processContext = threadLocalProcessContext.get();

		// Assert that the correct IDs have been set on the context.
		assertEquals("testProcessId", processContext.getProcessId());
		assertEquals("testDeploymentId", processContext.getDeploymentUnit());

		assertEquals("newTestValue", processContext.getChangedVariables().get("testVariableId"));

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
	}
	
	//TODO: Test that, once a ProcessContext has been created on the thread, we get the same one back on a second call to the listener.
	public void testThreadLocalProcessContext() {
		
		
	}
	

}
