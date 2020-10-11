package org.nachc.cad.cosmos.action.create.protocol.raw.util;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableGroupRecordIntegrationTestHelper {

	public static CreateProtocolRawDataParams getParams() {
		CreateProtocolRawDataParams params = new CreateProtocolRawDataParams();
		File file = getTestFile();
		params.setCreatedBy("greshje");
		params.setProtocolName("wmns_health");
		params.setProtocolNamePretty("Wmns's Health");
		params.setDataGroupName("Demographics");
		params.setDataGroupAbr("demo");
		params.setDatabricksFileLocation("/FileStore/tables/integration-test/womens-health/demo");
		params.setDatabricksFileName(file.getName());
		params.setFile(file);
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
		// delete the file from databricks
		log.info("Deleting databricks file: " + params.getDatabricksFilePath());
		DatabricksFileUtil util = DatabricksFileUtilFactory.get();
		DatabricksFileUtilResponse resp = util.delete(params.getDatabricksFilePath());
		log.info("Status: " + resp.getStatusCode());
		log.info("Success: " + resp.isSuccess());
		log.info("Response from delete: " + resp.getResponse());
		// drop the mysql stuff
		log.info("Dropping MySql stuff");
		Database.update("delete from raw_table_group where lower(code) = 'wmns_health_demo'", mySqlConn);
		log.info("Done with clean up");
	}

	private static File getTestFile() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\extracted\\AllianceChicago\\NACHC_UCSF_Patient_Demographic.txt";
		File file = new File(fileName);
		return file;
	}
	
}
