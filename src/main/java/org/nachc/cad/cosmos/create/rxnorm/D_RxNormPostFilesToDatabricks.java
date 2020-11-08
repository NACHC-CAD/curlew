package org.nachc.cad.cosmos.create.rxnorm;

import java.io.File;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class D_RxNormPostFilesToDatabricks {

	public static void post() {
		log.info("Starting upload to databricks");
		String databricksDir = A_RxNormParameters.DATABRICKS_DIR;
		String rrfDir = A_RxNormParameters.CSV_DIR;
		DatabricksFileUtilResponse resp;
		// upload conso
		File consoFile = new File(rrfDir, "RXNCONSO.RRF");
		String consoPath = databricksDir + "/conso";
		log.info("Uploading RXNORM CONSO: " + consoPath);
		resp = DatabricksFileUtilFactory.get().put(consoPath, consoFile);
		log.info(resp.isSuccess() + ": (" + resp.getStatusCode() + ") " + resp.getDatabricksFilePath() + "\t" + resp.getResponse());
		// upload rel
		File relFile = new File(rrfDir, "RXNREL.RRF");
		String relPath = databricksDir + "/rel";
		log.info("Uploading RXNORM REL: " + relPath);
		resp = DatabricksFileUtilFactory.get().put(relPath, relFile);
		log.info(resp.isSuccess() + ": (" + resp.getStatusCode() + ") " + resp.getDatabricksFilePath() + "\t" + resp.getResponse());
		// upload sat
		File satFile = new File(rrfDir, "RXNSAT.RRF");
		String satPath = databricksDir + "/sat";
		log.info("Uploading RXNORM SAT: " + satPath);
		resp = DatabricksFileUtilFactory.get().put(satPath, satFile);
		log.info(resp.isSuccess() + ": (" + resp.getStatusCode() + ") " + resp.getDatabricksFilePath() + "\t" + resp.getResponse());
		log.info("Done with upload to databricks");
	}

}
