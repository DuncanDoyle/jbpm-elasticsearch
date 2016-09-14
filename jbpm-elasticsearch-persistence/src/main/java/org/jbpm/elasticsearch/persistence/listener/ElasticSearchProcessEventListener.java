package org.jbpm.elasticsearch.persistence.listener;

import org.drools.persistence.TransactionManager;
import org.drools.persistence.TransactionSynchronization;
import org.jbpm.elasticsearch.client.ElasticSearchClient;
import org.jbpm.elasticsearch.client.RestEasyElasticSearchClient;
import org.jbpm.elasticsearch.persistence.context.ProcessEventContext;
import org.jbpm.elasticsearch.persistence.context.ProcessEventContext.ProcessState;
import org.jbpm.elasticsearch.persistence.model.JbpmProcessDocument;
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
 * <code>KIE</code> {@link ProcessEventListener}, which indexes process data in ElasticSearch using the {@link ElasticSearchClient}.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class ElasticSearchProcessEventListener extends DefaultProcessEventListener {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchProcessEventListener.class);

	private final ElasticSearchClient esClient;
	
	private ThreadLocal<ProcessEventContext> processContext = new ThreadLocal<>();

	public ElasticSearchProcessEventListener(final String elasticSearchEndpointUrl) {
		esClient =  new RestEasyElasticSearchClient(elasticSearchEndpointUrl);
	}

	@Override
	public void afterProcessStarted(ProcessStartedEvent event) {
		ProcessEventContext context = getProcessContext(event);
		context.setIndexingProcessState(ProcessState.STARTING);
		context.setProcessState(getProcessState(event));
	}

	@Override
	public void afterProcessCompleted(ProcessCompletedEvent event) {
		ProcessEventContext context = getProcessContext(event);
		context.setIndexingProcessState(ProcessState.COMPLETING);
		context.setProcessState(getProcessState(event));
	}

	@Override
	public void afterVariableChanged(ProcessVariableChangedEvent event) {
		ProcessEventContext context = getProcessContext(event);
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
	private ProcessEventContext getProcessContext(ProcessEvent processEvent) {
		ProcessEventContext context = processContext.get();
		if (context == null) {
			// Register a transaction synchronization to index the variables on a TX commit.
			registerTransactionSynchronization(processEvent);
			
			// Create a new context, and register for TX synchronization.
			long processInstanceId = processEvent.getProcessInstance().getId();
			LOGGER.debug("Building new IndexingContext for process-id: " + processInstanceId);
			String deploymentUnit = (String) processEvent.getKieRuntime().getEnvironment().get("deploymentId"); 
			String processId = processEvent.getProcessInstance().getProcessId();
			context = new ProcessEventContext(deploymentUnit, processId, processInstanceId);
			processContext.set(context);
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
			// TODO: Should we do something here?
		}

		@Override
		public void afterCompletion(int status) {
			LOGGER.debug("Indexing process after TX completion.");
			try {
				switch (status) {
				case TransactionManager.STATUS_COMMITTED:
					ProcessEventContext context = ElasticSearchProcessEventListener.this.processContext.get();
					if (context != null) {
						JbpmProcessDocument processDocument = new JbpmProcessDocument(context);
						String jsonProcessDocument = processDocument.toJsonString();
						String processDocumentId = processDocument.getDeploymentUnit() + "_" + processDocument.getProcessId() + "_" + processDocument.getProcessInstanceId();
						LOGGER.debug("Indexing Document: " + jsonProcessDocument);
						
						switch (context.getIndexingProcessState()) {
						case STARTING:
							esClient.indexProcessData(processDocumentId, jsonProcessDocument);
							break;
						case ACTIVE:
							esClient.updateProcessData(processDocumentId, jsonProcessDocument);
							break;
						case COMPLETING:
							// esProducer.delete
							// TODO: Should we update or remove here? I.e. do we want to maintain old processes in Elastic?
							esClient.updateProcessData(processDocumentId, jsonProcessDocument);
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
				processContext.set(null);
			}
		}
	}
	

}
