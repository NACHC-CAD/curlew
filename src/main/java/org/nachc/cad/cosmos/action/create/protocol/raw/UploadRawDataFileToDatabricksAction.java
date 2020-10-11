package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.exception.DatabricksFileException;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRawDataFileToDatabricksAction {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
		log.info("Writing file to databricks...");
		DatabricksFileUtil util = DatabricksFileUtilFactory.get();
		log.info("Writing file...");
		DatabricksFileUtilResponse resp = util.put(params.getDatabricksFileLocation(), params.getFile());
		log.info("Success: " + resp.isSuccess());
		log.info("Got response (" + resp.isSuccess() + ") " + resp.getElapsedSeconds() + " sec: " + resp.getFileName() + "\n" + resp.getResponse());
		if (resp.isSuccess() == false) {
			log.error("Error writing file to Databricks: " + params.getDatabricksFileLocation() + "/" + params.getDatabricksFileName());
			throw new DatabricksFileException(resp, "Did not get success response from server writing file: " + params.getDatabricksFileLocation() + "/" + params.getDatabricksFileName());
		}
	}
	
}
