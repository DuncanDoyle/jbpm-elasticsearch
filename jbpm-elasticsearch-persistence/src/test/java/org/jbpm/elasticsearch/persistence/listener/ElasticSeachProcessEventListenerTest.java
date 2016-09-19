package org.jbpm.elasticsearch.persistence.listener;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;

import org.drools.persistence.TransactionManager;
import org.drools.persistence.TransactionSynchronization;
import org.jbpm.elasticsearch.client.ElasticSearchClient;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;


/**
 * Test the {@link ElasticSearchProcessEventListener}.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
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
		assertThat(processContext.getProcessId(), is("testProcessId"));
		assertThat(processContext.getDeploymentUnit(), is("testDeploymentId"));

		// Assert the process state.
		assertThat(processContext.getProcessState(), is("ACTIVE"));
		assertThat(processContext.getIndexingProcessState(), is(ProcessState.STARTING));

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
		assertThat(processContext.getProcessId(), is("testProcessId"));
		assertThat(processContext.getDeploymentUnit(), is("testDeploymentId"));

		// Assert the process state.
		assertThat(processContext.getProcessState(), is("COMPLETED"));
		assertThat(processContext.getIndexingProcessState(), is(ProcessState.COMPLETING));

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
		assertThat(processContext.getProcessId(), is("testProcessId"));
		assertThat(processContext.getDeploymentUnit(), is("testDeploymentId"));
		assertThat(processContext.getChangedVariables().get("testVariableId"), is("newTestValue"));

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
	}

	// TODO: Test that, once a ProcessContext has been created on the thread, we get the same one back on a second call to the listener.
	public void testThreadLocalProcessContext() throws Exception {
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

		ProcessVariableChangedEvent variableChangedEvent = Mockito.mock(ProcessVariableChangedEvent.class);
		/*
		 * We just create another process instance here to verify that this does not get used when creating the context. I.e. we need to
		 * verify that this processInstance does not used when creating the ProcessContext.
		 */
		ProcessInstance anotherPInstance = Mockito.mock(ProcessInstance.class);
		Mockito.when(variableChangedEvent.getProcessInstance()).thenReturn(anotherPInstance);
		Mockito.when(anotherPInstance.getProcessId()).thenReturn("anotherProcessId");
		Mockito.when(anotherPInstance.getState()).thenReturn(ProcessInstance.STATE_ACTIVE);

		// Mock the variable change.
		Mockito.when(variableChangedEvent.getVariableId()).thenReturn("testVariableId");
		Mockito.when(variableChangedEvent.getNewValue()).thenReturn("newTestValue");

		listener.afterVariableChanged(variableChangedEvent);

		// Assert that the ProcessContext is the same when processing these 2 events in our listener.
		// Retrieve the context.
		Field threadLocalProcessContextField = listener.getClass().getDeclaredField("processContext");
		threadLocalProcessContextField.setAccessible(true);
		ThreadLocal<ProcessEventContext> threadLocalProcessContext = (ThreadLocal<ProcessEventContext>) threadLocalProcessContextField
				.get(listener);
		ProcessEventContext processContext = threadLocalProcessContext.get();

		// Context should have both the info from the afterProcessStartedEvent and the afterVariableChangedEvent.
		assertThat(processContext.getProcessId(), is("testProcesId"));
		assertThat(processContext.getChangedVariables().get("testVariableId"), is("newTestValue"));
	}

	@Test
	public void testTransactionSynchronizationProcessStartedAfterCompletion() throws Exception {
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
		// We use the ArgumentCaptor to capture the created transaction synchronization.
		ArgumentCaptor<TransactionSynchronization> tsArgumentCaptor = ArgumentCaptor.forClass(TransactionSynchronization.class);
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(environment.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		// Fire the event.
		listener.afterProcessStarted(event);

		// Get the transaction synchronization that has been created.
		Mockito.verify(tmManager).registerTransactionSynchronization(tsArgumentCaptor.capture());

		TransactionSynchronization ts = tsArgumentCaptor.getValue();

		// TODO: Now we can finally verify the ts.afterCompletion when the TS commits.
		// We first need to replace the ElasticSearchClient in the listener with a mock.
		ElasticSearchClient esClient = Mockito.mock(ElasticSearchClient.class);

		Field esClientField = ElasticSearchProcessEventListener.class.getDeclaredField("esClient");
		esClientField.setAccessible(true);
		esClientField.set(listener, esClient);

		ts.afterCompletion(TransactionManager.STATUS_COMMITTED);

		// Verify on the esClient mock that a document has been sent.
		verify(esClient, times(1)).indexProcessData(eq("testDeploymentId_testProcessId_0"), anyObject());
	}

	@Test
	public void testTransactionSynchronizationProcessActiveAfterCompletion() throws Exception {
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
		// We use the ArgumentCaptor to capture the created transaction synchronization.
		ArgumentCaptor<TransactionSynchronization> tsArgumentCaptor = ArgumentCaptor.forClass(TransactionSynchronization.class);
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(environment.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		listener.afterVariableChanged(event);

		// Get the transaction synchronization that has been created.
		Mockito.verify(tmManager).registerTransactionSynchronization(tsArgumentCaptor.capture());

		TransactionSynchronization ts = tsArgumentCaptor.getValue();

		// TODO: Now we can finally verify the ts.afterCompletion when the TS commits.
		// We first need to replace the ElasticSearchClient in the listener with a mock.
		ElasticSearchClient esClient = Mockito.mock(ElasticSearchClient.class);

		Field esClientField = ElasticSearchProcessEventListener.class.getDeclaredField("esClient");
		esClientField.setAccessible(true);
		esClientField.set(listener, esClient);

		ts.afterCompletion(TransactionManager.STATUS_COMMITTED);

		// Verify on the esClient mock that a document has been sent.
		verify(esClient, times(1)).updateProcessData(eq("testDeploymentId_testProcessId_0"), anyObject());

	}

	@Test
	public void testTransactionSynchronizationProcessCompletedAfterCompletion() throws Exception {
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
		// We use the ArgumentCaptor to capture the created transaction synchronization.
		ArgumentCaptor<TransactionSynchronization> tsArgumentCaptor = ArgumentCaptor.forClass(TransactionSynchronization.class);
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(environment.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		listener.afterProcessCompleted(event);

		// Get the transaction synchronization that has been created.
		Mockito.verify(tmManager).registerTransactionSynchronization(tsArgumentCaptor.capture());

		TransactionSynchronization ts = tsArgumentCaptor.getValue();

		// TODO: Now we can finally verify the ts.afterCompletion when the TS commits.
		// We first need to replace the ElasticSearchClient in the listener with a mock.
		ElasticSearchClient esClient = Mockito.mock(ElasticSearchClient.class);

		Field esClientField = ElasticSearchProcessEventListener.class.getDeclaredField("esClient");
		esClientField.setAccessible(true);
		esClientField.set(listener, esClient);

		ts.afterCompletion(TransactionManager.STATUS_COMMITTED);

		// Verify on the esClient mock that a document has been sent.
		verify(esClient, times(1)).updateProcessData(eq("testDeploymentId_testProcessId_0"), anyObject());
	}
	
	
}
