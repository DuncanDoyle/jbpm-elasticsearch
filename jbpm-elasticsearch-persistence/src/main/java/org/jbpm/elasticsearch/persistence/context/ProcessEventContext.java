package org.jbpm.elasticsearch.persistence.context;

import java.util.HashMap;
import java.util.Map;

/**
 * Keeps track of the <code>jBPM</code> process context during a transaction.
 * <p>
 * This class can be used during a <code>jBPM</code> transaction to store data we would like to index for our searches. For example:
 * <ul>
 * <li>deploymentUnit</li>
 * <li>processId</li>
 * <li>processInstanceId</li>
 * <li>processState</li>
 * </ul>
 * <p>
 * We need to keep track of this data during the transaction, as we only want to store the data when the transaction completes. Hence, the
 * various <code>jBPM<code> <code>EventListeners</code> need a context during the transaction to store their data.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class ProcessEventContext {

	public enum ProcessState {
		STARTING, ACTIVE, COMPLETING
	}

	private String deploymentUnit;

	private String processId;

	private long processInstanceId;

	private String processState;

	// Defaults to ACTIVE.
	private ProcessState indexingProcessState = ProcessState.ACTIVE;

	private Map<String, Object> changedVariables = new HashMap<>();

	public ProcessEventContext(String deploymentUnit, String processId, long processInstanceId) {
		this.deploymentUnit = deploymentUnit;
		this.processId = processId;
		this.processInstanceId = processInstanceId;
	}

	public void changeVariable(String key, Object value) {
		changedVariables.put(key, value);
	}

	public String getDeploymentUnit() {
		return deploymentUnit;
	}

	public void setDeploymentUnit(String deploymentUnit) {
		this.deploymentUnit = deploymentUnit;
	}

	public String getProcessId() {
		return processId;
	}

	public void setProcessId(String processId) {
		this.processId = processId;
	}

	public long getProcessInstanceId() {
		return processInstanceId;
	}

	public void setProcessInstanceId(long processInstanceId) {
		this.processInstanceId = processInstanceId;
	}

	public String getProcessState() {
		return processState;
	}

	public void setProcessState(String processState) {
		this.processState = processState;
	}

	public ProcessState getIndexingProcessState() {
		return indexingProcessState;
	}

	public void setIndexingProcessState(ProcessState processState) {
		this.indexingProcessState = processState;
	}

	public Map<String, Object> getChangedVariables() {
		return changedVariables;
	}

	public void setChangedVariables(Map<String, Object> changedVariables) {
		this.changedVariables = changedVariables;
	}

}
