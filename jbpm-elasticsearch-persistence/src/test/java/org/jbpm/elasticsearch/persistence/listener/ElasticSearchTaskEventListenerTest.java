package org.jbpm.elasticsearch.persistence.listener;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.drools.persistence.TransactionManager;
import org.jbpm.elasticsearch.persistence.context.TaskEventContext;
import org.jbpm.elasticsearch.persistence.context.TaskEventContext.TaskState;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.task.TaskEvent;
import org.kie.api.task.TaskLifeCycleEventListener;
import org.kie.api.task.model.OrganizationalEntity;
import org.kie.api.task.model.PeopleAssignments;
import org.kie.api.task.model.Status;
import org.kie.api.task.model.Task;
import org.kie.api.task.model.TaskData;
import org.kie.api.task.model.User;
import org.kie.internal.task.api.TaskContext;
import org.mockito.Mockito;

/**
 * Test the {@link ElasticSearchTaskEventListener}.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class ElasticSearchTaskEventListenerTest {

	private TaskLifeCycleEventListener listener;

	@Before
	public void before() {
		String elasticSearchEndpointUrl = "http://localhost:9800";
		listener = new ElasticSearchTaskEventListener(elasticSearchEndpointUrl);
	}

	@Test
	public void testAfterTaskAddedEvent() throws Exception {
		final Long testTaskId = 42L;
		final String testTaskName = "Test Task Added";
		final String testDeploymentId = "test-deployment-id";
		final String testProcessId = "test-process-id";
		final Long testProcessInstanceId = 112358L;
		// Create the key which is used to store the taskEventContext in the ThreadLocal Map.
		final String testTaskEventContextKey = new StringBuilder().append(testDeploymentId).append("-").append(testProcessId).append("-")
				.append(testProcessInstanceId).append("-").append(testTaskId).toString();

		final String testActualOwner = "Jason";
		final Date testCreatedOn = new Date();
		final Date testExpirationTime = new Date();

		final Set<String> testBAs = new HashSet<>();
		testBAs.add("Sinead");
		testBAs.add("Nadia");

		final Set<String> testPAs = new HashSet<>();
		testPAs.add("Duncan");
		testPAs.add("Alan");

		TaskEvent taskEvent = Mockito.mock(TaskEvent.class);
		Task task = Mockito.mock(Task.class);

		Mockito.when(taskEvent.getTask()).thenReturn(task);
		Mockito.when(task.getId()).thenReturn(testTaskId);
		Mockito.when(task.getName()).thenReturn(testTaskName);

		// Mock the (internal) TaskContext.
		TaskContext taskContext = Mockito.mock(TaskContext.class);
		Mockito.when(taskEvent.getTaskContext()).thenReturn(taskContext);

		// Mock the taskData
		TaskData taskData = Mockito.mock(TaskData.class);
		Mockito.when(task.getTaskData()).thenReturn(taskData);
		Mockito.when(taskData.getDeploymentId()).thenReturn(testDeploymentId);
		Mockito.when(taskData.getProcessId()).thenReturn(testProcessId);
		Mockito.when(taskData.getProcessInstanceId()).thenReturn(testProcessInstanceId);
		User user = Mockito.mock(User.class);
		Mockito.when(user.getId()).thenReturn(testActualOwner);
		Mockito.when(taskData.getActualOwner()).thenReturn(user);
		Mockito.when(taskData.getCreatedOn()).thenReturn(testCreatedOn);
		Mockito.when(taskData.getExpirationTime()).thenReturn(testExpirationTime);
		Mockito.when(taskData.getStatus()).thenReturn(Status.Created);
		PeopleAssignments pa = Mockito.mock(PeopleAssignments.class);
		Mockito.when(task.getPeopleAssignments()).thenReturn(pa);

		List<OrganizationalEntity> bas = new ArrayList<>();
		for (String nextBA : testBAs) {
			OrganizationalEntity nextOE = Mockito.mock(OrganizationalEntity.class);
			Mockito.when(nextOE.getId()).thenReturn(nextBA);
			bas.add(nextOE);
		}
		Mockito.when(pa.getBusinessAdministrators()).thenReturn(bas);

		List<OrganizationalEntity> pas = new ArrayList<>();
		for (String nextPA : testPAs) {
			OrganizationalEntity nextOE = Mockito.mock(OrganizationalEntity.class);
			Mockito.when(nextOE.getId()).thenReturn(nextPA);
			pas.add(nextOE);
		}
		Mockito.when(pa.getPotentialOwners()).thenReturn(pas);

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(taskContext.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		listener.afterTaskAddedEvent(taskEvent);

		// Retrieve the contexts.
		Field threadLocalTaskContextsField = listener.getClass().getDeclaredField("taskContexts");
		threadLocalTaskContextsField.setAccessible(true);
		ThreadLocal<Map<String, TaskEventContext>> threadLocalTaskContexts = (ThreadLocal<Map<String, TaskEventContext>>) threadLocalTaskContextsField
				.get(listener);
		// Retrieve the context for this specific task from the contexts Map set on the threadlocal.
		TaskEventContext taskEventContext = threadLocalTaskContexts.get().get(testTaskEventContextKey);

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());

		// Check the data set on the context.
		assertThat(taskEventContext.getActualOwner(), is(testActualOwner));
		assertThat(taskEventContext.getBusinessAdministrators(), is(testBAs));
		assertThat(taskEventContext.getCreatedOn(), is(testCreatedOn));
		assertThat(taskEventContext.getDeploymentUnit(), is(testDeploymentId));
		assertThat(taskEventContext.getExpirationTime(), is(testExpirationTime));
		assertThat(taskEventContext.getPotentialOwners(), is(testPAs));
		assertThat(taskEventContext.getProcessId(), is(testProcessId));
		assertThat(taskEventContext.getProcessInstanceId(), is(testProcessInstanceId));
		assertThat(taskEventContext.getTaskId(), is(testTaskId));
		assertThat(taskEventContext.getTaskName(), is(testTaskName));
		assertThat(taskEventContext.getTaskState(), is(TaskState.STARTING));
		assertThat(taskEventContext.getTaskStatus(), is(Status.Created.toString()));

	}

	// TODO: Test the creation of 2 tasks in one transaction.

	
	@Test
	public void testAfterTaskClaimedEvent() throws Exception {
		// We only test whethet the status has been set correctly. Testing the data is already done in "testAfterTaskAddedEvent" test
		final Long testTaskId = 43L;
		final String testTaskName = "Test Task Claimed";
		final String testDeploymentId = "test-deployment-id";
		final String testProcessId = "test-process-id";
		final Long testProcessInstanceId = 112358L;
		// Create the key which is used to store the taskEventContext in the ThreadLocal Map.
		final String testTaskEventContextKey = new StringBuilder().append(testDeploymentId).append("-").append(testProcessId).append("-")
				.append(testProcessInstanceId).append("-").append(testTaskId).toString();

		TaskEvent taskEvent = Mockito.mock(TaskEvent.class);
		Task task = Mockito.mock(Task.class);

		Mockito.when(taskEvent.getTask()).thenReturn(task);
		Mockito.when(task.getId()).thenReturn(testTaskId);
		Mockito.when(task.getName()).thenReturn(testTaskName);

		// Mock the (internal) TaskContext.
		TaskContext taskContext = Mockito.mock(TaskContext.class);
		Mockito.when(taskEvent.getTaskContext()).thenReturn(taskContext);

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(taskContext.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		// Mock the taskData
		TaskData taskData = Mockito.mock(TaskData.class);
		Mockito.when(task.getTaskData()).thenReturn(taskData);
		Mockito.when(taskData.getDeploymentId()).thenReturn(testDeploymentId);
		Mockito.when(taskData.getProcessId()).thenReturn(testProcessId);
		Mockito.when(taskData.getProcessInstanceId()).thenReturn(testProcessInstanceId);
		Mockito.when(taskData.getStatus()).thenReturn(Status.Reserved);

		
		PeopleAssignments pa = Mockito.mock(PeopleAssignments.class);
		Mockito.when(task.getPeopleAssignments()).thenReturn(pa);
		Mockito.when(pa.getPotentialOwners()).thenReturn(new ArrayList<OrganizationalEntity>());
		

		listener.afterTaskClaimedEvent(taskEvent);

		// Retrieve the contexts.
		Field threadLocalTaskContextField = listener.getClass().getDeclaredField("taskContexts");
		threadLocalTaskContextField.setAccessible(true);
		ThreadLocal<Map<String, TaskEventContext>> threadLocalTaskContexts = (ThreadLocal<Map<String, TaskEventContext>>) threadLocalTaskContextField
				.get(listener);
		// Retrieve the context for this specific task from the contexts Map set on the threadlocal.
		TaskEventContext taskEventContext = threadLocalTaskContexts.get().get(testTaskEventContextKey);

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
		assertThat(taskEventContext.getTaskStatus(), is(Status.Reserved.toString()));
	}

	@Test
	public void testAfterTaskCompletedEvent() throws Exception {
		final Long testTaskId = 44L;
		final String testTaskName = "Test Task Completed";
		final String testDeploymentId = "test-deployment-id";
		final String testProcessId = "test-process-id";
		final Long testProcessInstanceId = 112358L;
		// Create the key which is used to store the taskEventContext in the ThreadLocal Map.
		final String testTaskEventContextKey = new StringBuilder().append(testDeploymentId).append("-").append(testProcessId).append("-")
				.append(testProcessInstanceId).append("-").append(testTaskId).toString();

		TaskEvent taskEvent = Mockito.mock(TaskEvent.class);
		Task task = Mockito.mock(Task.class);

		Mockito.when(taskEvent.getTask()).thenReturn(task);
		Mockito.when(task.getId()).thenReturn(testTaskId);
		Mockito.when(task.getName()).thenReturn(testTaskName);

		// Mock the (internal) TaskContext.
		TaskContext taskContext = Mockito.mock(TaskContext.class);
		Mockito.when(taskEvent.getTaskContext()).thenReturn(taskContext);

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(taskContext.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		// Mock the taskData
		TaskData taskData = Mockito.mock(TaskData.class);
		Mockito.when(task.getTaskData()).thenReturn(taskData);
		Mockito.when(taskData.getDeploymentId()).thenReturn(testDeploymentId);
		Mockito.when(taskData.getProcessId()).thenReturn(testProcessId);
		Mockito.when(taskData.getProcessInstanceId()).thenReturn(testProcessInstanceId);
		Mockito.when(taskData.getStatus()).thenReturn(Status.Completed);

		
		PeopleAssignments pa = Mockito.mock(PeopleAssignments.class);
		Mockito.when(task.getPeopleAssignments()).thenReturn(pa);
		Mockito.when(pa.getPotentialOwners()).thenReturn(new ArrayList<OrganizationalEntity>());
		

		listener.afterTaskCompletedEvent(taskEvent);

		// Retrieve the contexts.
		Field threadLocalTaskContextField = listener.getClass().getDeclaredField("taskContexts");
		threadLocalTaskContextField.setAccessible(true);
		ThreadLocal<Map<String, TaskEventContext>> threadLocalTaskContexts = (ThreadLocal<Map<String, TaskEventContext>>) threadLocalTaskContextField
				.get(listener);
		// Retrieve the context for this specific task from the contexts Map set on the threadlocal.
		TaskEventContext taskEventContext = threadLocalTaskContexts.get().get(testTaskEventContextKey);

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
		assertThat(taskEventContext.getTaskStatus(), is(Status.Completed.toString()));
	}

	// TODO: Test a scenario where a task is completed and, within the same transaction, a task is added.

	public void testAfterTaskReleasedEvent() throws Exception {
		final Long testTaskId = 44L;
		final String testTaskName = "Test Task Released";
		final String testDeploymentId = "test-deployment-id";
		final String testProcessId = "test-process-id";
		final Long testProcessInstanceId = 112358L;
		// Create the key which is used to store the taskEventContext in the ThreadLocal Map.
		final String testTaskEventContextKey = new StringBuilder().append(testDeploymentId).append("-").append(testProcessId).append("-")
				.append(testProcessInstanceId).append("-").append(testTaskId).toString();

		TaskEvent taskEvent = Mockito.mock(TaskEvent.class);
		Task task = Mockito.mock(Task.class);

		Mockito.when(taskEvent.getTask()).thenReturn(task);
		Mockito.when(task.getId()).thenReturn(testTaskId);
		Mockito.when(task.getName()).thenReturn(testTaskName);

		// Mock the (internal) TaskContext.
		TaskContext taskContext = Mockito.mock(TaskContext.class);
		Mockito.when(taskEvent.getTaskContext()).thenReturn(taskContext);

		// We also need to mock the TM, as the "afterProcessCompleted" event will try to register a transactionsynchronization.
		TransactionManager tmManager = Mockito.mock(TransactionManager.class);
		Mockito.when(taskContext.get(EnvironmentName.TRANSACTION_MANAGER)).thenReturn(tmManager);

		// Mock the taskData
		TaskData taskData = Mockito.mock(TaskData.class);
		Mockito.when(task.getTaskData()).thenReturn(taskData);
		Mockito.when(taskData.getDeploymentId()).thenReturn(testDeploymentId);
		Mockito.when(taskData.getProcessId()).thenReturn(testProcessId);
		Mockito.when(taskData.getProcessInstanceId()).thenReturn(testProcessInstanceId);
		Mockito.when(taskData.getStatus()).thenReturn(Status.Ready);

		
		PeopleAssignments pa = Mockito.mock(PeopleAssignments.class);
		Mockito.when(task.getPeopleAssignments()).thenReturn(pa);
		Mockito.when(pa.getPotentialOwners()).thenReturn(new ArrayList<OrganizationalEntity>());
		

		listener.afterTaskCompletedEvent(taskEvent);

		// Retrieve the contexts.
		Field threadLocalTaskContextField = listener.getClass().getDeclaredField("taskContexts");
		threadLocalTaskContextField.setAccessible(true);
		ThreadLocal<Map<String, TaskEventContext>> threadLocalTaskContexts = (ThreadLocal<Map<String, TaskEventContext>>) threadLocalTaskContextField
				.get(listener);
		// Retrieve the context for this specific task from the contexts Map set on the threadlocal.
		TaskEventContext taskEventContext = threadLocalTaskContexts.get().get(testTaskEventContextKey);

		// Assert that the TransactionSynchronization got registered with the TM.
		verify(tmManager, times(1)).registerTransactionSynchronization(anyObject());
		assertThat(taskEventContext.getTaskStatus(), is(Status.Ready.toString()));
	}

}
