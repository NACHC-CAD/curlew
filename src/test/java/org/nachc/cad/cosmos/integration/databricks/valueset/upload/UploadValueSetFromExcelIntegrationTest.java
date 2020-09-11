package org.nachc.cad.cosmos.integration.databricks.valueset.upload;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadValueSetFromExcelIntegrationTest {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		File file = getTestFile();
		log.info("Got file: " + file.exists() + FileUtil.getCanonicalPath(file));
		log.info("Done.");
	}

	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/excel");
		List<File> files = FileUtil.listFiles(dir, "*.xlsx");
		return files.get(0);
	}

}
