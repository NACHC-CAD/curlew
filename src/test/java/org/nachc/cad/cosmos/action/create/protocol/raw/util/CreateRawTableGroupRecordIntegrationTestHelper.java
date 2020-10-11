package org.nachc.cad.cosmos.action.create.protocol.raw.util;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableGroupRecordIntegrationTestHelper {

	public static CreateProtocolRawDataParams getParams() {
		CreateProtocolRawDataParams params = new CreateProtocolRawDataParams();
		params.setCreatedBy("greshje");
		params.setProtocolName("wmns_health");
		params.setProtocolNamePretty("Wmns's Health");
		params.setDataGroupName("Demographics");
		params.setDataGroupAbr("demo");
		return params;
	}

	public static void cleanUp(CreateProtocolRawDataParams params, Connection mySqlConn, Connection dbConn) {
		// drop the databricks stuff
		String databaseName;
		// drop prj schema
		databaseName = "prj_grp_" + params.getProtocolName();
		log.info("Dropping databricks schema: " + databaseName);
		DatabricksDbUtil.dropDatabase(databaseName, dbConn);
		// drop raw schema
		databaseName = "prj_raw_" + params.getProtocolName();
		log.info("Dropping databricks schema: " + databaseName);
		DatabricksDbUtil.dropDatabase(databaseName, dbConn);
		// drop the mysql stuff
		log.info("Dropping MySql stuff");
		Database.update("delete from raw_table_group where lower(code) = 'wmns_health_demo'", mySqlConn);
		log.info("Done with clean up");
	}

}
