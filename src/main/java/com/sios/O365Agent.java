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
		
		// 環境変数からAzure Queue Storageへの接続情報を取得する。
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

		// キューのクライアントを作成する。
		CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

		// o365-requestキューへの参照情報を取得する。
		CloudQueue requestQueue = queueClient.getQueueReference("o365-request");

		// o365-responseキューへの参照情報を取得する。
		CloudQueue responseQueue = queueClient.getQueueReference("o365-response");

		while (true) {
			// o365-requestキューからメッセージを取得する。
			CloudQueueMessage retrievedMessage = requestQueue.retrieveMessage();
			if (retrievedMessage != null) {
				
				// o365-requestキューのメッセージを取得する。
				String requestJSONString = retrievedMessage.getMessageContentAsString();

				// o365-requestキューをオブジェクトに変換する。
				ObjectMapper mapper = new ObjectMapper();
				RequestJSON requestJSON = mapper.readValue(requestJSONString, RequestJSON.class);

				// o365-responseキューへ格納するメッセージのオブジェクトを作成する。
				ResponseJSON responseJSON = new ResponseJSON();
				responseJSON.setTaskId(requestJSON.getTaskId());
				responseJSON.setUserId(requestJSON.getUserId());

				try {
					// デキューが5回以上したら、そのキューを削除する。
					if (retrievedMessage.getDequeueCount() >= 5) {
						requestQueue.deleteMessage(retrievedMessage);
					}
					System.out.println(retrievedMessage.getMessageContentAsString());

					// Office365へのアカウント追加処理(今回は省略しています)
					Thread.sleep(1000);

					// 処理成功なので、o365-responseキューへ格納するメッセージの処理結果を
					// 成功として返す。
					responseJSON.setProcessState("02");

					// o365-responseにキューを配信する。
					String processingJSONString = mapper.writeValueAsString(responseJSON);

					CloudQueueMessage processingMessage = new CloudQueueMessage(processingJSONString);
					responseQueue.addMessage(processingMessage);

				} catch (Exception e) {

					e.printStackTrace();
					// 処理失敗なので、o365-responseキューへ格納するメッセージの処理結果を
					// 失敗として返す。
					responseJSON.setProcessState("03");

					// o365-responseにキューを配信する。
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
