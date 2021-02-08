package org.nachc.cad.cosmos.init.rxnorm.init;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.database.Database;
import org.yaorma.util.time.Timer;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRxNorm {

	@Test
	public void shouldUploadFiles() {
		log.info("Starting test...");
		log.info("Deleteting RXNORM Schema");
		CosmosConnections conns = new CosmosConnections();
		DatabricksDbUtil.dropDatabase("rxnorm", conns.getDbConnection(), conns);
		log.info("Closing database");
		conns.close();
		log.info("Doing file uploads...");
		String path = DatabricksParams.getRxNormDatabricksDir();
		String currentPath = "";
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		Timer timer = new Timer();
		// remove old files
		log.info("Removing old dir: " + path);
		timer.getStart();
		util.rmdir(path);
		timer.getStop();
		log.info("RMDIR: \t" + timer.getElapsedString());
		// conso
		currentPath = path + "/rxnconso";
		log.info("CONSO: uploading conso");
		log.info("Path: " + currentPath);
		timer.start();
		util.replace(currentPath, getFile("RXNCONSO.RRF"));
		timer.stop();
		log.info("CONSO: \t" + timer.getElapsedString());
		// sat
		currentPath = path + "/rxnsat";
		timer.start();
		log.info("SAT:   uploading sat");
		util.replace(currentPath, getFile("RXNSAT.RRF"));
		timer.stop();
		log.info("SAT: \t" + timer.getElapsedString());
		// rel
		currentPath = path + "/rxnrel";
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
		log.info("Size: " + file.length());
		return file;
	}

}
