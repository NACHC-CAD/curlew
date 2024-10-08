package org.nachc.cad.cosmos.integration.databricks.file;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.http.HttpRequestClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadFileAsStreamIntegrationTest {

	private static final File FILE = new File("C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-04-02-COVID-DemoRaceNachc\\demo_race_nachc\\race_mappings_2021_03_31_RU.csv");
	
	@Test
	public void shouldLoadFile() {
		// set up the util class
		log.info("Posting file...");
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		String databricksDirPath = "/FileStore/tables/integration-test/delete-me";
		String fileName = "delete-me-test-file.csv";
		String filePath = databricksDirPath + "/" + fileName;
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		// check for the file
		log.info("Checking for exists");
		DatabricksFileUtilResponse resp = util.exists(filePath);
		boolean exists = resp.isFileExists();
		log.info("Exists: " + exists);
		// delete if exists
		if(exists) {
			log.info("Deleteing");
			resp = util.delete(filePath);
			log.info("Deleted: " + resp.isSuccess());
		}
		// post the file
		resp = util.putLargeFile(databricksDirPath, FILE);
		// echo the response
		log.info("Got response: \n" + resp.getResponse());
		// get the file back from the server
		log.info("Getting file from server...");
		resp = util.get(databricksDirPath + "/" + FILE.getName());
		log.info("Success: " + resp.isSuccess());
		log.info("Status:  " + resp.getStatusCode());
		log.info("Got response: " + resp.getResponse());
		log.info("Response: \n" + resp.getResponse());
		String str = FileUtil.getAsString(resp.getInputStream());
		log.info("Got file contents: \n" + str);
		assertTrue(str.indexOf("BLACK") > 0);
		log.info("Done.");
	}

}
