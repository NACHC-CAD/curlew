package org.nachc.cad.cosmos.integration.databricks.file;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadFileIntegration {

	@Test
	public void shouldLoadFile() {
		log.info("Starting test...");
		// get the test file
		log.info("Getting file and params");
		File file = getTestFile();
		InputStream in = FileUtil.getInputStream(file);
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
		if (exists) {
			log.info("Deleteing");
			resp = util.delete(filePath);
			log.info("Deleted: " + resp.isSuccess());
		}
		// delete if it exists
		// post the file
		resp = util.put(databricksDirPath, in, fileName, true);
		// echo the response
		log.info("Got response: \n" + resp.getResponse());
		// get the file back from the server
		log.info("Getting file from server...");
		resp = util.get(databricksDirPath + "/" + fileName);
		log.info("Success: " + resp.isSuccess());
		log.info("Status:  " + resp.getStatusCode());
		log.info("Got response: " + resp.getResponse());
		log.info("Response: \n" + resp.getResponse());
		String str = FileUtil.getAsString(resp.getInputStream());
		log.info("Got file contents: \n" + str);
		assertTrue(str.indexOf("SNOMED") > 0);
		log.info("Done.");
	}

	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/csv");
		List<File> files = FileUtil.listFiles(dir, "*.*");
		File file = files.get(0);
		return file;
	}

}
