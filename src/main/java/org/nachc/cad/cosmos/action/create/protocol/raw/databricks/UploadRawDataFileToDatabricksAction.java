package org.nachc.cad.cosmos.action.create.protocol.raw.databricks;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.exception.DatabricksFileException;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRawDataFileToDatabricksAction {

	public static void execute(RawDataFileUploadParams params, boolean isOverwrite) {
		log.info("Writing file to databricks...");
		DatabricksFileUtil util = DatabricksFileUtilFactory.get();
		if (isOverwrite == true) {
			log.info("* * * OVERWRITING ANY EXISTING FILE * * *");
			log.info("Deleting existing file: " + params.getDatabricksFilePath());
			DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().delete(params.getDatabricksFilePath());
			log.info("Done with delete (" + resp.isSuccess() + "): \n" + resp.getResponse());
		}
		log.info("Writing file...");
		DatabricksFileUtilResponse resp = util.put(params.getDatabricksFilePathWithoutFileName(), params.getFile());
		log.info("Success: " + resp.isSuccess());
		log.info("Got response (" + resp.isSuccess() + ") " + resp.getElapsedSeconds() + " sec: " + resp.getFileName() + "\n" + resp.getResponse());
		if (resp.isSuccess() == false) {
			log.error("Error writing file to Databricks: " + params.getDatabricksFilePath());
			throw new DatabricksFileException(resp, "Did not get success response from server writing file: " + params.getDatabricksFilePath());
		}
	}

}
