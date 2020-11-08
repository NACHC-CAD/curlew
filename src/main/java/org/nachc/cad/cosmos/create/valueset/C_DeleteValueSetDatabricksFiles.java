package org.nachc.cad.cosmos.create.valueset;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class C_DeleteValueSetDatabricksFiles {

	public static void deleteFiles() {
		String dirName;
		DatabricksFileUtilResponse resp;
		// delete csv files
		dirName = A_ParametersForValueSetSchema.DATABRICKS_FILE_PATH;
		log.info("Deleting from: " + dirName);
		resp = DatabricksFileUtilFactory.get().rmdir(dirName);
		log.info(resp.isSuccess() + ": (" + resp.getStatusCode() + ") " + resp.getDatabricksFilePath() + "\t" + resp.getResponse());
		// delete meta file
		dirName = A_ParametersForValueSetSchema.DATABRICKS_META_FILE_PATH;
		log.info("Deleting from: " + dirName);
		resp = DatabricksFileUtilFactory.get().rmdir(dirName);
		log.info(resp.isSuccess() + ": (" + resp.getStatusCode() + ") " + resp.getDatabricksFilePath() + "\t" + resp.getResponse());
		log.info("Done with delete");
	}

}
