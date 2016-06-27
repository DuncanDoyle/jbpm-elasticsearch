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
import org.jbpm.elasticsearch.persistence.listener.IndexingTaskContext.TaskState;
import org.jbpm.services.task.events.DefaultTaskEventListener;
import org.kie.api.event.process.ProcessEventListener;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.task.TaskEvent;
import org.kie.api.task.model.OrganizationalEntity;
import org.kie.api.task.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>KIE</code> {@link ProcessEventListener}, which indexes human-task-variables in ElasticSearch using the {@link ElasticSearchClient}.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class IndexingTaskEventListener extends DefaultTaskEventListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTaskEventListener.class);

	// We use a Map of IndexTaskContexts as multiple tasks can be created in a single transaction.
	private ThreadLocal<Map<String, IndexingTaskContext>> indexingContext = new ThreadLocal<>();

	// TODO Make the ES endpoint configurable on the constructor of this listener.
	private ElasticSearchClient esClient = new RestEasyElasticSearchClient("http://elasticsearch:9200");

	@Override
	public void afterTaskAddedEvent(TaskEvent event) {
		IndexingTaskContext context = getIndexingContext(event);
		context.setIndexingTaskState(TaskState.STARTING);
		setTaskIndexingData(event, context);
	}

	@Override
	public void afterTaskClaimedEvent(TaskEvent event) {
		IndexingTaskContext context = getIndexingContext(event);
		setTaskIndexingData(event, context);
	}

	@Override
	public void afterTaskCompletedEvent(TaskEvent event) {
		IndexingTaskContext context = getIndexingContext(event);
		setTaskIndexingData(event, context);
	}

	@Override
	public void afterTaskReleasedEvent(TaskEvent event) {
		IndexingTaskContext context = getIndexingContext(event);
		setTaskIndexingData(event, context);
	}

	private void setTaskIndexingData(TaskEvent event, IndexingTaskContext context) {
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
	private IndexingTaskContext getIndexingContext(TaskEvent taskEvent) {
		long taskId = taskEvent.getTask().getId();
		Map<String, IndexingTaskContext> contexts = indexingContext.get();
		if (contexts == null) {
			// Register a transaction synchronization to index the variables on a TX commit.
			registerTransactionSynchronization(taskEvent);

			contexts = new HashMap<>();
			indexingContext.set(contexts);
		}
		IndexingTaskContext context = contexts.get(Long.toString(taskId));
		if (context == null) {
			// Create a new context.
			long processInstanceId = taskEvent.getTask().getTaskData().getProcessInstanceId();
			LOGGER.debug("Building new IndexingTaskContext for process-id: '" + processInstanceId + "' and task-id '" + taskId + "'.");
			String deploymentUnit = (String) taskEvent.getTask().getTaskData().getDeploymentId();
			String processId = taskEvent.getTask().getTaskData().getProcessId();
			context = new IndexingTaskContext(deploymentUnit, processId, processInstanceId, taskId);
			contexts.put(Long.toString(taskId), context);
		}
		return context;
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
			// TODO: Check whether we need to do something here. Currently I think we don't have to do anything here.
		}

		@Override
		public void afterCompletion(int status) {
			// Build up the JSON document.
			LOGGER.debug("Indexing process after TX completion.");
			try {
				switch (status) {
				case TransactionManager.STATUS_COMMITTED:
					// Loop through all contexts and write to ElasticSearch for each task.
					Map<String, IndexingTaskContext> contexts = IndexingTaskEventListener.this.indexingContext.get();
					if (contexts != null) {
						Collection<IndexingTaskContext> contextValues = contexts.values();

						for (IndexingTaskContext nextContext : contextValues) {

							IndexingTaskDocument indexingDocument = new IndexingTaskDocument(nextContext);
							String jsonIndexingDocument = indexingDocument.toJsonString();
							String indexId = indexingDocument.getDeploymentUnit() + "_" + indexingDocument.getProcessId() + "_"
									+ indexingDocument.getProcessInstanceId() + "_" + indexingDocument.getTaskId();
							LOGGER.debug("Indexing Document: " + jsonIndexingDocument);

							switch (nextContext.getIndexingTaskState()) {
							case STARTING:
								esClient.indexTaskData(indexId, jsonIndexingDocument);
								break;
							case ACTIVE:
								esClient.updateTaskData(indexId, jsonIndexingDocument);
								break;
							case COMPLETING:
								// esProducer.delete
								// TODO: Should we update or remove here? I.e. do we want to maintain old processes in Elastic?
								esClient.updateTaskData(indexId, jsonIndexingDocument);
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
				indexingContext.set(null);
			}
		}
	}

}
