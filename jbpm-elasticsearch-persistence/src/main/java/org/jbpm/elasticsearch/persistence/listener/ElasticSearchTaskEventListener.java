package org.jbpm.elasticsearch.persistence.listener;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.drools.persistence.TransactionManager;
import org.drools.persistence.TransactionSynchronization;
import org.jbpm.elasticsearch.client.ElasticSearchClient;
import org.jbpm.elasticsearch.client.RestEasyElasticSearchClient;
import org.jbpm.elasticsearch.persistence.context.TaskEventContext;
import org.jbpm.elasticsearch.persistence.context.TaskEventContext.TaskState;
import org.jbpm.elasticsearch.persistence.model.JbpmTaskDocument;
import org.jbpm.services.task.events.DefaultTaskEventListener;
import org.kie.api.event.process.ProcessEventListener;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.task.TaskEvent;
import org.kie.api.task.model.OrganizationalEntity;
import org.kie.api.task.model.TaskData;
import org.kie.api.task.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>KIE</code> {@link ProcessEventListener}, which indexes human-task data in ElasticSearch using the {@link ElasticSearchClient}.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class ElasticSearchTaskEventListener extends DefaultTaskEventListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTaskEventListener.class);

	private final ElasticSearchClient esClient;

	// We use a Map of IndexTaskContexts as multiple tasks can be created in a single transaction.
	private ThreadLocal<Map<String, TaskEventContext>> taskContexts = new ThreadLocal<>();

	public ElasticSearchTaskEventListener(final String elasticSearchEndpointUrl) {
		esClient = new RestEasyElasticSearchClient(elasticSearchEndpointUrl);
	}

	@Override
	public void afterTaskAddedEvent(TaskEvent event) {
		TaskEventContext context = getTaskContext(event);
		context.setTaskState(TaskState.STARTING);
		setTaskData(event, context);
	}

	@Override
	public void afterTaskClaimedEvent(TaskEvent event) {
		TaskEventContext context = getTaskContext(event);
		setTaskData(event, context);
	}

	@Override
	public void afterTaskCompletedEvent(TaskEvent event) {
		TaskEventContext context = getTaskContext(event);
		setTaskData(event, context);
	}

	@Override
	public void afterTaskReleasedEvent(TaskEvent event) {
		TaskEventContext context = getTaskContext(event);
		setTaskData(event, context);
	}

	private void setTaskData(TaskEvent event, TaskEventContext context) {
		context.setTaskStatus(event.getTask().getTaskData().getStatus().toString());
		context.setTaskName(event.getTask().getName());
		Set<String> potentialOwners = new HashSet<>();
		// List<O>
		List<OrganizationalEntity> potentialOwnersList = event.getTask().getPeopleAssignments().getPotentialOwners();
		for (OrganizationalEntity nextPO : potentialOwnersList) {
			potentialOwners.add(nextPO.getId());
		}
		context.setPotentialOwners(potentialOwners);

		Set<String> businessAdministrators = new HashSet<>();
		List<OrganizationalEntity> businessAdminList = event.getTask().getPeopleAssignments().getBusinessAdministrators();
		for (OrganizationalEntity nextBA : businessAdminList) {
			businessAdministrators.add(nextBA.getId());
		}
		context.setBusinessAdministrators(businessAdministrators);

		User actualOwner = event.getTask().getTaskData().getActualOwner();
		if (actualOwner != null) {
			context.setActualOwner(actualOwner.getId());
		}

		context.setCreatedOn(event.getTask().getTaskData().getCreatedOn());

		context.setExpirationTime(event.getTask().getTaskData().getExpirationTime());
	}

	// We don't need to synchronize as we're working on a threadlocal.
	private TaskEventContext getTaskContext(TaskEvent taskEvent) {
		// We use a Map of taskContexts because a single process can contain multiple tasks. We track the events per task.
		Map<String, TaskEventContext> contexts = taskContexts.get();
		if (contexts == null) {
			// Register a transaction synchronization to index the variables on a TX commit.
			registerTransactionSynchronization(taskEvent);

			contexts = new HashMap<>();
			taskContexts.set(contexts);
		}

		TaskEventContext context = contexts.get(getTaskEventContextKey(taskEvent));
		if (context == null) {
			// Create a new context.
			long processInstanceId = taskEvent.getTask().getTaskData().getProcessInstanceId();
			long taskId = taskEvent.getTask().getId();
			LOGGER.debug("Building new TaskContext for process-id: '" + processInstanceId + "' and task-id '" + taskId + "'.");
			String deploymentUnit = (String) taskEvent.getTask().getTaskData().getDeploymentId();
			String processId = taskEvent.getTask().getTaskData().getProcessId();
			context = new TaskEventContext(deploymentUnit, processId, processInstanceId, taskId);
			contexts.put(getTaskEventContextKey(taskEvent), context);
		}
		return context;
	}

	/**
	 * Computes the key with which we can store the taskEventContext in our Map.
	 * <p/>
	 * Uses more than just the TaskId. The reason for this is that we can pontentially have 2 processes with tasks with the same id starting
	 * in the same transaction. An example is when we use sub-processes and we start a human-task and a subprocess with a humantask with the
	 * same-id in parallel.
	 * 
	 * @param taskEvent
	 *            the {@link TaskEvent}
	 * @return the computed key
	 */
	private String getTaskEventContextKey(TaskEvent taskEvent) {
		TaskData taskData = taskEvent.getTask().getTaskData();
		return new StringBuilder().append(taskData.getDeploymentId()).append("-").append(taskData.getProcessId()).append("-")
				.append(taskData.getProcessInstanceId()).append("-").append(taskEvent.getTask().getId()).toString();
	}

	private void registerTransactionSynchronization(TaskEvent taskEvent) {
		TransactionManager tmManager = (TransactionManager) ((org.kie.internal.task.api.TaskContext) taskEvent.getTaskContext())
				.get(EnvironmentName.TRANSACTION_MANAGER);
		if (tmManager == null) {
			String message = "This task event listener requires access to a TransactionManager.";
			LOGGER.error(message);
			throw new IllegalStateException(message);
		}
		// Check that there is a transaction on the thread.
		if (tmManager.getStatus() == TransactionManager.STATUS_NO_TRANSACTION) {
			String message = "No transaction!";
			LOGGER.error(message);
			throw new IllegalStateException(message);
		}

		TransactionSynchronizationAdapter tsAdapter = new TransactionSynchronizationAdapter();
		tmManager.registerTransactionSynchronization(tsAdapter);
	}

	private class TransactionSynchronizationAdapter implements TransactionSynchronization {

		@Override
		public void beforeCompletion() {
			// TODO: Should we do something here?
		}

		@Override
		public void afterCompletion(int status) {
			LOGGER.debug("Indexing process after TX completion.");
			try {
				switch (status) {
				case TransactionManager.STATUS_COMMITTED:
					// Loop through all contexts and write to ElasticSearch for each task.
					Map<String, TaskEventContext> contexts = ElasticSearchTaskEventListener.this.taskContexts.get();
					if (contexts != null) {
						Collection<TaskEventContext> contextValues = contexts.values();

						for (TaskEventContext nextContext : contextValues) {

							JbpmTaskDocument jbpmTaskDocument = new JbpmTaskDocument(nextContext);
							String jsonIndexingDocument = jbpmTaskDocument.toJsonString();
							String taskDocumentId = jbpmTaskDocument.getDeploymentUnit() + "_" + jbpmTaskDocument.getProcessId() + "_"
									+ jbpmTaskDocument.getProcessInstanceId() + "_" + jbpmTaskDocument.getTaskId();
							LOGGER.debug("Indexing Document: " + jsonIndexingDocument);

							switch (nextContext.getTaskState()) {
							case STARTING:
								esClient.indexTaskData(taskDocumentId, jsonIndexingDocument);
								break;
							case ACTIVE:
								esClient.updateTaskData(taskDocumentId, jsonIndexingDocument);
								break;
							case COMPLETING:
								// esProducer.delete
								// TODO: Should we update or remove here? I.e. do we want to maintain old processes in Elastic?
								esClient.updateTaskData(taskDocumentId, jsonIndexingDocument);
								break;
							default:
								String message = "Unexpected process state.";
								LOGGER.error(message);
								throw new IllegalStateException(message);
							}
						}
					}
					break;
				case TransactionManager.STATUS_ROLLEDBACK:
					LOGGER.warn("Transaction rolled back. Discarding process indexing for this transaction.");
					break;
				default:
					String message = "Unexpected transaction outcome.";
					LOGGER.error(message);
					throw new IllegalStateException(message);
				}
			} finally {
				// Reset the ThreadLocal.
				taskContexts.set(null);
			}
		}
	}

}
