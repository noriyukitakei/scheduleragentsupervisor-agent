package com.sios;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;

public class O365Agent {

	public static final String storageConnectionString = System.getenv("STORAGE_KEY");
	public static final Charset charset = StandardCharsets.UTF_8;

	public static void main(String[] args)
			throws InvalidKeyException, URISyntaxException, StorageException, InterruptedException, JsonParseException, JsonMappingException, IOException {
		// Retrieve storage account from connection-string.
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

		// Create the queue client.
		CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

		// Retrieve a reference to a queue.
		CloudQueue requestQueue = queueClient.getQueueReference("o365-request");

		CloudQueue responseQueue = queueClient.getQueueReference("o365-response");

		while (true) {

			CloudQueueMessage retrievedMessage = requestQueue.retrieveMessage();
			if (retrievedMessage != null) {
				String requestJSONString = retrievedMessage.getMessageContentAsString();

				ObjectMapper mapper = new ObjectMapper();
				RequestJSON requestJSON = mapper.readValue(requestJSONString, RequestJSON.class);

				ResponseJSON responseJSON = new ResponseJSON();
				responseJSON.setTaskId(requestJSON.getTaskId());
				responseJSON.setUserId(requestJSON.getUserId());

				try {
					if (retrievedMessage.getDequeueCount() >= 5) {
						System.out.println("デキュー5回以上");
						requestQueue.deleteMessage(retrievedMessage);
					}
					System.out.println(retrievedMessage.getMessageContentAsString());

					// Office365へのアカウント追加処理
					Thread.sleep(1000);
					String hoge = null;
					System.out.println(hoge.length());

					// 処理成功
					responseJSON.setProcessState("02");

					String processingJSONString = mapper.writeValueAsString(responseJSON);

					CloudQueueMessage processingMessage = new CloudQueueMessage(processingJSONString);
					responseQueue.addMessage(processingMessage);

				} catch (Exception e) {
					// 処理失敗
					e.printStackTrace();
					responseJSON.setProcessState("03");

					String processedJSONString = mapper.writeValueAsString(responseJSON);

					CloudQueueMessage processedMessage = new CloudQueueMessage(processedJSONString);
					responseQueue.addMessage(processedMessage);
					
				} finally {
					requestQueue.deleteMessage(retrievedMessage);
				}
			}
		}
	}

}
