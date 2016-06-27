package org.jbpm.elasticsearch.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>RESTEasy</code> based implementation of the <code>ElasticSearch</code> client. 
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class RestEasyElasticSearchClient implements ElasticSearchClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(RestEasyElasticSearchClient.class);

	private String elasticEndpoint;
	
	public RestEasyElasticSearchClient(String elasticEndpoint) {
		this.elasticEndpoint = elasticEndpoint;
	}
	
	@Override
	public void indexProcessData(String id, String processData) {
		LOGGER.debug("Indexing process-data with id: '" + id + "' and data: " + processData);
		StringBuilder endpointBuilder = new StringBuilder(elasticEndpoint);
		String index = "bpmsuite";
		String type = "process";
		endpointBuilder.append("/").append(index).append("/").append(type).append("/").append(id);
		String endpoint = endpointBuilder.toString();
		LOGGER.debug("Sending insert request to endpoint: " + endpoint);
		post(endpoint, processData);
		post(endpointBuilder.toString(), processData);
	}
	
	@Override
	public void updateProcessData(String id, String processData) {
		LOGGER.debug("Indexing process-data with id: '" + id + "' and data: " + processData);
		StringBuilder endpointBuilder = new StringBuilder(elasticEndpoint);
		String index = "bpmsuite";
		String type = "process";
		String operation = "_update";
		endpointBuilder.append("/").append(index).append("/").append(type).append("/").append(id).append("/").append(operation);
		String endpoint = endpointBuilder.toString();
		LOGGER.debug("Sending update request to endpoint: " + endpoint);
		post(endpoint, wrapForUpdate(processData));
	}
	
	private String wrapForUpdate(String document) {
		StringBuilder updateDocBuilder = new StringBuilder();
		updateDocBuilder.append("{ \"doc\":").append(document).append("}");
		return updateDocBuilder.toString();
	}

	@Override
	public void indexTaskData(String id, String taskData) {
		LOGGER.debug("Indexing task-data with id: '" + id + "' and data: " + taskData);
		StringBuilder endpointBuilder = new StringBuilder(elasticEndpoint);
		String index = "bpmsuite";
		String type = "task";
		endpointBuilder.append("/").append(index).append("/").append(type).append("/").append(id);
		String endpoint = endpointBuilder.toString();
		LOGGER.debug("Sending insert request to endpoint: " + endpoint);
		post(endpoint, taskData);
		post(endpointBuilder.toString(), taskData);
	}
	
	@Override
	public void updateTaskData(String id, String taskData) {
		LOGGER.debug("Indexing task-data with id: '" + id + "' and data: " + taskData);
		StringBuilder endpointBuilder = new StringBuilder(elasticEndpoint);
		String index = "bpmsuite";
		String type = "task";
		String operation = "_update";
		endpointBuilder.append("/").append(index).append("/").append(type).append("/").append(id).append("/").append(operation);
		String endpoint = endpointBuilder.toString();
		LOGGER.debug("Sending update request to endpoint: " + endpoint);
		post(endpoint, wrapForUpdate(taskData));
	}

	private String post(String endpoint, String data) {
		String output = "";
		try {

			ClientRequest request = new ClientRequest(endpoint);
			request.accept(MediaType.APPLICATION_JSON);
			request.body(MediaType.APPLICATION_JSON, data);
			ClientResponse<String> response = request.post(String.class);
			LOGGER.debug("ElasticSearch response-status: " + response.getStatus());
			if (response.getStatus() >= 400) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(response.getEntity().getBytes())));
			output = IOUtils.toString(br);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return output;
	}

	

}
