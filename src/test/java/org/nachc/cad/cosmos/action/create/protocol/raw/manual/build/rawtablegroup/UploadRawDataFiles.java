package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchemaAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadRawDataFiles {

	public static void createNewEntity(RawDataFileUploadParams params, CosmosConnections conns, boolean isOverwrite) {
		exec(params, conns, true, isOverwrite);
	}

	public static void updateExistingEntity(RawDataFileUploadParams params, CosmosConnections conns, boolean isOverwrite) {
		exec(params, conns, false, isOverwrite);
	}

	private static void exec(RawDataFileUploadParams params, CosmosConnections conns, boolean isNewProject, boolean isOverwrite) {
		log.info("Starting test...");
		log.info("Getting localhost dir");
		String rootDir = params.getLocalHostFileAbsLocation();
		log.info("Getting connections");
		// create the project if this is a new project
		if (isNewProject == true) {
			createProject(params, conns);
		}
		// upload the files
		List<File> files = getFiles(rootDir);
		for (File file : files) {
			log.info("File: " + FileUtil.getCanonicalPath(file));
			updateParamsWithFileInfo(params, file);
			AddRawDataFileAction.execute(params, conns.getDbConnection(), conns.getMySqlConnection(), isOverwrite);
		}
		log.info("Done.");

	}

	private static List<File> getFiles(String rootDirName) {
		File rootDir = new File(rootDirName);
		List<File> rtn = FileUtil.listFiles(rootDir, "**/*");
		return rtn;
	}

	private static void createProject(RawDataFileUploadParams params, CosmosConnections conns) {
		createDatabricksSchemaForProject(conns, params);
		createMySqlSchemaForProject(conns, params);
	}

	private static void createDatabricksSchemaForProject(CosmosConnections conns, RawDataFileUploadParams params) {
		// clear the files off of data bricks
		DatabricksFileUtilFactory.get().rmdir(params.getDatabricksFileLocation());
		// drop the raw and grp databases in data bricks
		DatabricksDbUtil.dropDatabase(params.getGroupTableSchemaName(), conns.getDbConnection());
		DatabricksDbUtil.dropDatabase(params.getRawTableSchemaName(), conns.getDbConnection());
		// create the raw and grp databases in data bricks
		CreateRawDataDatabricksSchemaAction.execute(params, conns.getDbConnection());
	}

	private static void createMySqlSchemaForProject(CosmosConnections conns, RawDataFileUploadParams params) {
		// delete the old
		DeleteRawDataGroupAction.delete(params.getRawTableGroupCode(), conns);
		// create a new empty raw data group
		CreateRawTableGroupRecordAction.execute(params, conns.getMySqlConnection());
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
