package org.nachc.cad.cosmos.integration.databricks.rxnorm.init;

import java.io.File;
import java.util.List;

import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRxNorm {

	public static void main(String[] args) {
		String path = DatabricksParams.getRxNormDatabricksDir();
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		// conso
		log.info("CONSO: uploading conso");
		util.replace(path + "/conso", getFile("RXNCONSO.RRF"));
		// rel
		log.info("SAT:   uploading sat");
		util.replace(path + "/sat", getFile("RXNSAT.RRF"));
		// sat
		log.info("REL:   uploading rel");
		util.replace(path + "/rel", getFile("RXNREL.RRF"));
		log.info("Done.");
	}
	
	private static File getFile(String name) {
		String dirName = DatabricksParams.getRxNormRrfDir();
		File dir = new File(dirName);
		List<File> files = FileUtil.listFiles(dir, name);
		File file = files.get(0);
		log.info("File:  " + FileUtil.getCanonicalPath(file));
		return file;
	}
	
}
