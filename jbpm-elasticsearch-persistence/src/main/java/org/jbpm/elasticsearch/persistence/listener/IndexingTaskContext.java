package org.jbpm.elasticsearch.persistence.listener;

import java.util.Date;
import java.util.Set;

/**
 * Keeps track of the <code>jBPM</code> human-task context during a transaction.
 * <p>
 * This class can be used during a <code>jBPM</code> transaction to store human-task related data we would like to index for our searches. For example:
 * <ul>
 * <li>deploymentUnit</li>
 * <li>processId</li>
 * <li>taskId</li>
 * <li>taskStatus</li>
 * <li>etc</li>
 * </ul>
 * <p>
 * We need to keep track of this data during the transaction, as we only want to store the data when the transaction completes. Hence, the
 * various <code>jBPM<code> <code>EventListeners</code> need a context during the transaction to store their data.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class IndexingTaskContext {
	
	public enum TaskState {
		STARTING,
		ACTIVE,
		COMPLETING
	}

	private String deploymentUnit;
	
	private String processId;
	
	private long processInstanceId;
	
	private long taskId;
	
	private String taskStatus;

	private TaskState indexingTaskState = TaskState.ACTIVE;
	
	private String taskName;
	
	private Set<String> potentialOwners;
	
	private Set<String> businessAdministrators;
	
	private String actualOwner;
	
	private Date createdOn;
	
	private Date expirationTime;
	
	
	public IndexingTaskContext(String deploymentUnit, String processId, long processInstanceId, long taskId) {
		this.deploymentUnit = deploymentUnit;
		this.processId = processId;
		this.processInstanceId = processInstanceId;
		this.taskId = taskId;
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

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public String getTaskStatus() {
		return taskStatus;
	}

	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}

	public TaskState getIndexingTaskState() {
		return indexingTaskState;
	}

	public void setIndexingTaskState(TaskState indexingTaskState) {
		this.indexingTaskState = indexingTaskState;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public Set<String> getPotentialOwners() {
		return potentialOwners;
	}

	public void setPotentialOwners(Set<String> potentialOwners) {
		this.potentialOwners = potentialOwners;
	}

	public Set<String> getBusinessAdministrators() {
		return businessAdministrators;
	}

	public void setBusinessAdministrators(Set<String> businessAdministrators) {
		this.businessAdministrators = businessAdministrators;
	}

	public String getActualOwner() {
		return actualOwner;
	}

	public void setActualOwner(String actualOwner) {
		this.actualOwner = actualOwner;
	}

	public Date getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(Date createdOn) {
		this.createdOn = createdOn;
	}

	public Date getExpirationTime() {
		return expirationTime;
	}

	public void setExpirationTime(Date expirationTime) {
		this.expirationTime = expirationTime;
	}
	
		
}
