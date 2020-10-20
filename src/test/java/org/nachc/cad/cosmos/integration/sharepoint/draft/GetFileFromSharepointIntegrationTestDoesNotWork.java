package org.nachc.cad.cosmos.integration.sharepoint.draft;

import org.junit.Test;

import com.nach.core.util.http.HttpRequestClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetFileFromSharepointIntegrationTestDoesNotWork {

	@Test
	public void shouldGetFile() {
		log.info("Starting test...");
		String url = "https://nachc.sharepoint.com/sites/MillionHearts/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FMillionHearts%2FShared%20Documents%2FGeneral%2FYear%202%20%2D%202019%2D2020%2FData%20and%20Reporting%20%2D%2008312019%20%2D%2007312020&FolderCTID=0x012000F1B819F53E12194A83A4F872E6A3C008";
		String uid = "greshje@gmail.com";
		String pwd = "Stripedbass01!";
		HttpRequestClient client = new HttpRequestClient(url);
		client.addNtlmAuth(uid, pwd, "guest");
		log.info("Doing get");
		client.doGet();
		log.info("Got response code: " + client.getStatusCode());
		log.info("Response: \n" + client.getResponse());
		log.info("Done.");
		
	}
	
}
