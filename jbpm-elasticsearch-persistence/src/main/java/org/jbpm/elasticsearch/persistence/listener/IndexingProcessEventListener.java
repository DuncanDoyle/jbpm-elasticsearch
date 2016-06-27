package org.jbpm.elasticsearch.persistence.listener;

import org.drools.persistence.TransactionManager;
import org.drools.persistence.TransactionSynchronization;
import org.jbpm.elasticsearch.client.ElasticSearchClient;
import org.jbpm.elasticsearch.client.RestEasyElasticSearchClient;
import org.jbpm.elasticsearch.persistence.listener.IndexingProcessContext.ProcessState;
import org.kie.api.event.process.DefaultProcessEventListener;
import org.kie.api.event.process.ProcessCompletedEvent;
import org.kie.api.event.process.ProcessEvent;
import org.kie.api.event.process.ProcessEventListener;
import org.kie.api.event.process.ProcessStartedEvent;
import org.kie.api.event.process.ProcessVariableChangedEvent;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.process.ProcessInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>KIE</code> {@link ProcessEventListener}, which indexes process-variables in ElasticSearch using the {@link ElasticSearchClient}.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 *
 */
public class IndexingProcessEventListener extends DefaultProcessEventListener {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexingProcessEventListener.class);

	private ThreadLocal<IndexingProcessContext> indexingContext = new ThreadLocal<>();

	// TODO Make the ES endpoint configurable on the constructor of this listener.
	private ElasticSearchClient esClient = new RestEasyElasticSearchClient("http://elasticsearch:9200");

	@Override
	public void afterProcessStarted(ProcessStartedEvent event) {
		IndexingProcessContext context = getIndexingContext(event);
		context.setIndexingProcessState(ProcessState.STARTING);
		context.setProcessState(getProcessState(event));
	}

	@Override
	public void afterProcessCompleted(ProcessCompletedEvent event) {
		IndexingProcessContext context = getIndexingContext(event);
		context.setIndexingProcessState(ProcessState.COMPLETING);
		context.setProcessState(getProcessState(event));
	}

	@Override
	public void afterVariableChanged(ProcessVariableChangedEvent event) {
		IndexingProcessContext context = getIndexingContext(event);
		context.changeVariable(event.getVariableId(), event.getNewValue());
		context.setProcessState(getProcessState(event));
	}
	
	private String getProcessState(ProcessEvent event) {
		String stateName = "";
		int state = event.getProcessInstance().getState();
		switch(state) {
			case ProcessInstance.STATE_ABORTED:
				stateName = "ABORTED";
				break;
			case ProcessInstance.STATE_ACTIVE:
				stateName = "ACTIVE";
				break;
			case ProcessInstance.STATE_COMPLETED:
				stateName = "COMPLETED";
				break;
			case ProcessInstance.STATE_PENDING:
				stateName = "PENDING";
				break;
			case ProcessInstance.STATE_SUSPENDED:
				stateName = "SUSPENDED";
				break;
		}
		return stateName;
	}
	
	

	// We don't need to synchronize as we're working on a threadlocal.
	private IndexingProcessContext getIndexingContext(ProcessEvent processEvent) {
		IndexingProcessContext context = indexingContext.get();
		if (context == null) {
			// Register a transaction synchronization to index the variables on a TX commit.
			registerTransactionSynchronization(processEvent);
			
			// Create a new context, and register for TX synchronization.
			long processInstanceId = processEvent.getProcessInstance().getId();
			LOGGER.debug("Building new IndexingContext for process-id: " + processInstanceId);
			String deploymentUnit = (String) processEvent.getKieRuntime().getEnvironment().get("deploymentId"); 
			String processId = processEvent.getProcessInstance().getProcessId();
			context = new IndexingProcessContext(deploymentUnit, processId, processInstanceId);
			indexingContext.set(context);
		}
		return context;
	}

	private void registerTransactionSynchronization(ProcessEvent processEvent) {
		TransactionManager tmManager = (TransactionManager) processEvent.getKieRuntime().getEnvironment()
				.get(EnvironmentName.TRANSACTION_MANAGER);
		if (tmManager == null) {
			String message = "This process event listener requires access to a TransactionManager.";
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
					IndexingProcessContext context = IndexingProcessEventListener.this.indexingContext.get();
					if (context != null) {
						IndexingProcessDocument indexingDocument = new IndexingProcessDocument(context);
						String jsonIndexingDocument = indexingDocument.toJsonString();
						String indexId = indexingDocument.getDeploymentUnit() + "_" + indexingDocument.getProcessId() + "_" + indexingDocument.getProcessInstanceId();
						LOGGER.debug("Indexing Document: " + jsonIndexingDocument);
						
						switch (context.getIndexingProcessState()) {
						case STARTING:
							esClient.indexProcessData(indexId, jsonIndexingDocument);
							break;
						case ACTIVE:
							esClient.updateProcessData(indexId, jsonIndexingDocument);
							break;
						case COMPLETING:
							// esProducer.delete
							// TODO: Should we update or remove here? I.e. do we want to maintain old processes in Elastic?
							esClient.updateProcessData(indexId, jsonIndexingDocument);
							break;
						default:
							String message = "Unexpected process state.";
							LOGGER.error(message);
							throw new IllegalStateException(message);
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
