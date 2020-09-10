package org.nachc.cad.cosmos.integration.databricks.file;

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

	@Test
	public void shouldLoadFile() {
		log.info("Starting test...");
		log.info("Getting file and params");
		File file = getTestFile();
		InputStream in = FileUtil.getInputStream(file);
		log.info("Posting file...");
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		String databricksDirPath = "/FileStore/tables/integration-test/delete-me";
		String fileName = "delete-me-test-file.csv";
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		DatabricksFileUtilResponse response = util.put(databricksDirPath, in, fileName);
		log.info("Got response: \n" + response.getResponse());
		log.info("Done.");
	}

	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/csv");
		List<File> files = FileUtil.listFiles(dir, "*.*");
		File file = files.get(0);
		return file;
	}

}
