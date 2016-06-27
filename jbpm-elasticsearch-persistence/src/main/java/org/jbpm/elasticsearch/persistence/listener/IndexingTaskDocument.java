package org.jbpm.elasticsearch.persistence.listener;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java representation of our JSON human-task data we want to index for search.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class IndexingTaskDocument {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTaskDocument.class);
	
	private String deploymentUnit;

	private String processId;

	private long processInstanceId;

	private long taskId;

	private String taskStatus;
	
	private String taskName;
	
	private Set<String> potentialOwners;
	
	private Set<String> businessAdministrators;
	
	private String actualOwner;
	
	private Date createdOn;
	
	private Date expirationTime;
	
	private ObjectMapper jsonObjectMapper = new ObjectMapper().setSerializationInclusion(Inclusion.NON_EMPTY);
	
	public IndexingTaskDocument(IndexingTaskContext context) {
		this.deploymentUnit = context.getDeploymentUnit();
		this.processId = context.getProcessId();
		this.processInstanceId = context.getProcessInstanceId();
		this.taskId = context.getTaskId();
		this.taskStatus = context.getTaskStatus();
		this.taskName = context.getTaskName();
		this.potentialOwners = context.getPotentialOwners();
		this.businessAdministrators = context.getBusinessAdministrators();
		this.actualOwner = context.getActualOwner();
		this.createdOn = context.getCreatedOn();
		this.expirationTime = context.getExpirationTime();
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

	public String toJsonString() {
		String jsonString;
		try {
			jsonString = jsonObjectMapper.writeValueAsString(this);
		} catch (IOException e) {
			String message = "Unable to marshall object to JSON string.";
			LOGGER.error(message, e);
			//This will probably end up rolling back the DB transaction and thus the process.
			throw new RuntimeException(message, e);
		}
		return jsonString;
	}

}
