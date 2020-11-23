package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.group.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchemaAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRawDataFiles {

	public static void createNewEntity(RawDataFileUploadParams params, boolean isOverwrite) {
		exec(params, true, isOverwrite);
	}
	
	public static void updateExistingEntity(RawDataFileUploadParams params, boolean isOverwrite) {
		exec(params, false, isOverwrite);
	}
	
	private static void exec(RawDataFileUploadParams params, boolean isNewProject, boolean isOverwrite) {
		log.info("Starting test...");
		log.info("Getting localhost dir");
		String rootDir = params.getLocalHostFileAbsLocation();
		log.info("Getting connections");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		// create the project if this is a new project
		if(isNewProject == true) {
			createProject(params, mySqlConn, dbConn);
		}
		// upload the files
		List<File> files = getFiles(rootDir);
		for (File file : files) {
			log.info("File: " + FileUtil.getCanonicalPath(file));
			updateParamsWithFileInfo(params, file);
			AddRawDataFileAction.execute(params, dbConn, mySqlConn, isOverwrite);
		}
		log.info("Creating group table");
		CreateGrpDataTableAction.execute(params, dbConn, mySqlConn);
		log.info("Done.");

	}

	private static List<File> getFiles(String rootDirName) {
		File rootDir = new File(rootDirName);
		List<File> rtn = FileUtil.listFiles(rootDir, "**/*");
		return rtn;
	}

	private static void createProject(RawDataFileUploadParams params, Connection mySqlConn, Connection dbConn) {
		createDatabricksSchemaForProject(mySqlConn, dbConn, params);
		createMySqlSchemaForProject(mySqlConn, dbConn, params);
	}

	private static void createDatabricksSchemaForProject(Connection mySqlConn, Connection dbConn, RawDataFileUploadParams params) {
		// clear the files off of data bricks
		DatabricksFileUtilFactory.get().rmdir(params.getDatabricksFileLocation());
		// drop the raw and grp databases in data bricks
		DatabricksDbUtil.dropDatabase(params.getGroupTableSchemaName(), dbConn);
		DatabricksDbUtil.dropDatabase(params.getRawTableSchemaName(), dbConn);
		// create the raw and grp databases in data bricks
		CreateRawDataDatabricksSchemaAction.execute(params, dbConn);
	}

	private static void createMySqlSchemaForProject(Connection mySqlConn, Connection dbConn, RawDataFileUploadParams params) {
		// delete the old
		DeleteRawDataGroupAction.delete(params.getRawTableGroupCode(), dbConn, mySqlConn);
		// create a new empty raw data group
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
	}

	private static void updateParamsWithFileInfo(RawDataFileUploadParams params, File file) {
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
