package org.nachc.cad.cosmos.action.create.protocol.raw.util;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableGroupRecordIntegrationTestHelper {

	/**
	 * 
	 * Method to get test parameters for integration tests.
	 * 
	 */
	public static CreateProtocolRawDataParams getParams() {
		CreateProtocolRawDataParams params = new CreateProtocolRawDataParams();
		File file = getTestFile();
		params.setCreatedBy("greshje");
		params.setOrgCode("ac");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		params.setDataGroupName("Demographics");
		params.setDataGroupAbr("demo");
		params.setDatabricksFileLocation("/FileStore/tables/integration-test/womens-health/demo");
		params.setDelimiter('|');
		params.setFileName(file.getName());
		params.setFile(file);
		return params;
	}

	/**
	 * 
	 * Method to remove records created using the above parameters.  
	 * 
	 */
	public static void cleanUp(CreateProtocolRawDataParams params, Connection mySqlConn, Connection dbConn) {
		log.info("Doing clean up...");
		cleanUpDatabricks(params, dbConn);
		cleanupMySql(params, mySqlConn);
		log.info("Done with clean up");
	}

	/**
	 * 
	 * Cleanup for databricks. 
	 * 
	 */
	public static void cleanUpDatabricks(CreateProtocolRawDataParams params, Connection dbConn) {
		// drop the databricks stuff
		String databaseName;
		// drop prj schema
		databaseName = "prj_grp_" + params.getProjCode();
		log.info("Dropping databricks schema: " + databaseName);
		DatabricksDbUtil.dropDatabase(databaseName, dbConn);
		// drop raw schema
		databaseName = "prj_raw_" + params.getProjCode();
		log.info("Dropping databricks schema: " + databaseName);
		DatabricksDbUtil.dropDatabase(databaseName, dbConn);
		// delete the file from databricks
		log.info("Deleting databricks file: " + params.getDatabricksFilePath());
		DatabricksFileUtil util = DatabricksFileUtilFactory.get();
		DatabricksFileUtilResponse resp = util.rmdir(params.getDatabricksFileLocation());
		log.info("Status: " + resp.getStatusCode());
		log.info("Success: " + resp.isSuccess());
		log.info("Response from delete: " + resp.getResponse());
	}

	/**
	 * 
	 * Cleanup for mysql
	 * 
	 */
	public static void cleanupMySql(CreateProtocolRawDataParams params, Connection mySqlConn) {
		// drop the mysql stuff
		log.info("Dropping MySql stuff");
		String rawTableGuid = getRawTableGuid(params, mySqlConn);
		if (rawTableGuid != null) {
			Database.update("delete from raw_table_col where raw_table = ?", new String[] { rawTableGuid }, mySqlConn);
		}
		Database.update("delete from raw_table_group_raw_table where raw_table = ?", new String[] { rawTableGuid }, mySqlConn);
		Database.update("delete from raw_table_file where file_location = ? and file_name = ?", new String[] { params.getDatabricksFileLocation(), params.getDatabricksFileName() }, mySqlConn);
		Database.update("delete from raw_table_file where file_location = ? and file_name = ?", new String[] { params.getDatabricksFileLocation(), params.getDatabricksFileName() }, mySqlConn);
		Database.update("delete from raw_table where raw_table_schema = ? and raw_table_name = ?", new String[] { params.getRawTableSchemaName(), params.getRawTableName() }, mySqlConn);
		Database.update("delete from raw_table_group where lower(code) = ?", params.getRawTableGroupCode(), mySqlConn);
	}

	//
	// file used for tests (called by the getParams() method above)
	//
	
	private static File getTestFile() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\demo\\ac\\THUBMNAIL_10_NACHC_UCSF_Patient_Demographic.txt";
		File file = new File(fileName);
		return file;
	}

	//
	// method to get the guid back from the database
	//
	
	private static String getRawTableGuid(CreateProtocolRawDataParams params, Connection mySqlConn) {
		RawTableDvo dvo = Dao.find(new RawTableDvo(), new String[] { "raw_table_schema", "raw_table_name" }, new String[] { params.getRawTableSchemaName(), params.getRawTableName() }, mySqlConn);
		if (dvo != null) {
			return dvo.getGuid();
		} else {
			return null;
		}
	}
}
