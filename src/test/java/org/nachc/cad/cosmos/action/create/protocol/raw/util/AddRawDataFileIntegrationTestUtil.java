package org.nachc.cad.cosmos.action.create.protocol.raw.util;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.mysql.drop.DropMySqlSchema;
import org.nachc.cad.cosmos.mysql.update.UpdateMySql;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddRawDataFileIntegrationTestUtil {

	/**
	 * 
	 * Method to get test parameters for integration tests.
	 * 
	 */
	public static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
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
	public static void cleanUp(RawDataFileUploadParams params, Connection mySqlConn, Connection dbConn) {
		log.info("Doing clean up...");
		burnDatabricksToTheGround(dbConn);
		cleanUpMySql(params, mySqlConn);
		log.info("Done with clean up");
	}

	/**
	 * 
	 * Cleanup for databricks.
	 * 
	 */
	public static void burnDatabricksToTheGround(Connection dbConn) {
		DatabricksFileUtilFactory.get().rmdir("/FileStore/tables");
		List<String> schema = DatabricksDbUtil.listRawSchema(dbConn);
		for (String str : schema) {
			log.info("Dropping schema: " + str);
			DatabricksDbUtil.dropDatabase(str, dbConn);
		}
	}

	public static void burnMySqlToTheGround(Connection mySqlConn) {
		Database.update("drop schema if exists cosmos", mySqlConn);
		UpdateMySql.update(mySqlConn);
	}

	/**
	 * 
	 * Cleanup for mysql
	 * 
	 */
	public static void cleanUpMySql(RawDataFileUploadParams params, Connection mySqlConn) {
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

	private static String getRawTableGuid(RawDataFileUploadParams params, Connection mySqlConn) {
		RawTableDvo dvo = Dao.find(new RawTableDvo(), new String[] { "raw_table_schema", "raw_table_name" }, new String[] { params.getRawTableSchemaName(), params.getRawTableName() }, mySqlConn);
		if (dvo != null) {
			return dvo.getGuid();
		} else {
			return null;
		}
	}
}
