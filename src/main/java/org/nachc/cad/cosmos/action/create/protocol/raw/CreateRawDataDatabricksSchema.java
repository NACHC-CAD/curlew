package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawDataDatabricksSchema {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
		// drop the databricks stuff
		String databaseName;
		// drop prj schema
		databaseName = "prj_grp_" + params.getProjCode();
		log.info("Creating databricks schema: " + databaseName);
		DatabricksDbUtil.createDatabase(databaseName, conn);
		// drop raw schema
		databaseName = "prj_raw_" + params.getProjCode();
		log.info("Creating databricks schema: " + databaseName);
		DatabricksDbUtil.createDatabase(databaseName, conn);
	}

}
