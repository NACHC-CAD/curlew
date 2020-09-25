package org.nachc.cad.cosmos.init.rxnorm.init;

import java.io.File;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.util.time.Timer;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRxNorm {

	@Test
	public void shouldUploadFiles() {
		log.info("Starting test...");
		String path = DatabricksParams.getRxNormDatabricksDir();
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		Timer timer = new Timer();
		// remove old files
		log.info("Removing old dir");
		timer.getStart();
		util.rmdir(path);
		timer.getStop();
		log.info("RMDIR: \t" + timer.getElapsedString());
		// conso
		log.info("CONSO: uploading conso");
		timer.start();
		util.replace(path + "/rxnconso", getFile("RXNCONSO.RRF"));
		timer.stop();
		log.info("CONSO: \t" + timer.getElapsedString());
		// rel
		timer.start();
		log.info("SAT:   uploading sat");
		util.replace(path + "/rxnsat", getFile("RXNSAT.RRF"));
		timer.stop();
		log.info("SAT: \t" + timer.getElapsedString());
		// sat
		log.info("REL:   uploading rel");
		timer.start();
		util.replace(path + "/rxnrel", getFile("RXNREL.RRF"));
		timer.stop();
		log.info("REL: \t" + timer.getElapsedString());
		log.info("Done.");
	}

	private File getFile(String name) {
		String dirName = DatabricksParams.getRxNormRrfDir();
		File dir = new File(dirName);
		List<File> files = FileUtil.listFiles(dir, name);
		File file = files.get(0);
		log.info("File:  " + FileUtil.getCanonicalPath(file));
		return file;
	}

}
