package org.nachc.cad.cosmos.create.rxnorm;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class B_RxNormDeleteDatabricksFiles {

	public static void delete() {
		String dirName = A_RxNormParameters.DATABRICKS_DIR;
		log.info("Deleting from: " + dirName);
		DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().rmdir(dirName);
		log.info(resp.isSuccess() + ": (" + resp.getStatusCode() + ") " + resp.getDatabricksFilePath() + "\t" + resp.getResponse());
		log.info("Done with delete");
	}

}
