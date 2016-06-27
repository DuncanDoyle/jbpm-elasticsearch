package org.jbpm.elasticsearch.client;


/**
 * API to put <code>jBPM</code> process and human-task data into <code>ElasticSearch</code>. 
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public interface ElasticSearchClient {

	public void indexProcessData(String id, String processData);
	
	public void updateProcessData(String id, String processData);
	
	public void indexTaskData(String id, String taskData);
	
	public void updateTaskData(String id, String taskData);
	
}
