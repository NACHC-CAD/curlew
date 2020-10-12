package org.nachc.cad.cosmos.action.create.protocol.raw.mysql;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawDataDatabricksSchema {

	public static void execute(RawDataFileUploadParams params, Connection conn) {
		// drop the databricks stuff
		String databaseName;
		// drop prj schema
		databaseName = params.getRawTableSchemaName();
		log.info("Creating databricks schema: " + databaseName);
		DatabricksDbUtil.createDatabase(databaseName, conn);
		// drop raw schema
		databaseName = params.getGroupTableSchemaName();
		log.info("Creating databricks schema: " + databaseName);
		DatabricksDbUtil.createDatabase(databaseName, conn);
	}

}
