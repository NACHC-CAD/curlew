package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.impl;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.group.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchema;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddAllFilesToDatabricks {

	public static void AddAllFiles(RawDataFileUploadParams params, String rootDir) {
		log.info("Starting test...");
		log.info("Getting connections");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		// do the set up (this adds data that would already exists in the system)
		init(params, mySqlConn, dbConn);
		// do the upload
		List<File> files = getFiles(rootDir);
		for (File file : files) {
			log.info("File: " + FileUtil.getCanonicalPath(file));
			updateParams(params, file);
			AddRawDataFileAction.execute(params, dbConn, mySqlConn);
		}
		log.info("Done.");

	}

	private static List<File> getFiles(String rootDirName) {
		File rootDir = new File(rootDirName);
		List<File> rtn = FileUtil.listFiles(rootDir, "**/*");
		return rtn;
	}

	private static void init(RawDataFileUploadParams params, Connection mySqlConn, Connection dbConn) {
		cleanUpDatabricks(mySqlConn, dbConn, params);
		cleanUpMySql(mySqlConn, dbConn, params);
	}

	private static void cleanUpDatabricks(Connection mySqlConn, Connection dbConn, RawDataFileUploadParams params) {
		// clear the files off of data bricks
		DatabricksFileUtilFactory.get().rmdir(params.getDatabricksFileLocation());
		// drop the raw and grp databases in data bricks
		DatabricksDbUtil.dropDatabase(params.getGroupTableSchemaName(), dbConn);
		DatabricksDbUtil.dropDatabase(params.getRawTableSchemaName(), dbConn);
		// create the raw and grp databases in data bricks
		CreateRawDataDatabricksSchema.execute(params, dbConn);
	}

	private static void cleanUpMySql(Connection mySqlConn, Connection dbConn, RawDataFileUploadParams params) {
		// delete the old
		DeleteRawDataGroupAction.delete(params.getRawTableGroupCode(), dbConn, mySqlConn);
		// create a new empty raw data group
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
	}

	private static void updateParams(RawDataFileUploadParams params, File file) {
		params.setFileName(file.getName());
		params.setFile(file);
		params.setOrgCode(file.getParentFile().getName());
		if (file.getName().toLowerCase().endsWith(".txt")) {
			params.setDelimiter('|');
		} else {
			params.setDelimiter(',');
		}
	}

}
