package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.hiv;

import java.io.File;
import java.util.List;

import org.nachc.cad.cosmos.action.create.metrics.CreateMetricsTable;
import org.nachc.cad.cosmos.action.create.project.CreateProjectAction;
import org.nachc.cad.cosmos.action.create.project.UploadFilesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateColumnMappingsAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildHiv {

	private static final String PROJECT_CODE = "hiv";

	private static final String PROJECT_NAME = "HIV Prevention";

	private static final String PROJECT_DESC = "HIV Prevention Project";

	private static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/";

	private static final String LOT1_SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\hiv\\2021-02-27-HIV\\";

	public static final String PATTERN = "_meta\\*.xlsx";

	public static final String SQL_FILE_PATTERN = "_sql\\*.sql";

	//
	// main method (see exec method below for implementation)
	//

	public static void main(String[] args) {
		log.info("Starting main...");
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("executing...");
			exec(conns);
			log.info("Committing");
			conns.commit();
			log.info("Done with commit");
		} finally {
			log.info("Closing connections...");
			conns.close();
			log.info("Done with close");
		}
		log.info("Done.");
	}

	/**
	 * 
	 * This method will delete and rebuild the project.
	 * 
	 */

	public static void exec(CosmosConnections conns) {
		// delete and recreate the project
		deleteProject(conns);
		createProject(conns);
		// data file uploads
		uploadLot1(conns);
		// create column mappings and group table
		createColumnMappings(conns);
		createGroupTables(conns);
		buildSchema(conns);
	}

	//
	// all private past here
	//

	private static void deleteProject(CosmosConnections conns) {
		DeleteProjectAction.exec("hiv", DATABRICKS_FILE_ROOT + "hiv", conns);
	}

	private static void createProject(CosmosConnections conns) {
		RawDataFileUploadParams params = getParams();
		CreateProjectAction.exec(params, conns);
	}

	private static void uploadLot1(CosmosConnections conns) {
		RawDataFileUploadParams params = getParams();
		params.setLocalHostFileRootDir(LOT1_SRC_ROOT);
		UploadFilesAction.exec("Demographics", "demo", "2021-02", params, conns);
		UploadFilesAction.exec("Encounter", "enc", "2021-02", params, conns);
	}

	private static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode(PROJECT_CODE);
		params.setProtocolNamePretty(PROJECT_NAME);
		params.setProjName(PROJECT_NAME);
		params.setProjDescription(PROJECT_DESC);
		params.setDatabricksFileRoot(DATABRICKS_FILE_ROOT);
		return params;
	}

	public static void createColumnMappings(CosmosConnections conns) {
		log.info("Creating column aliases...");
		List<File> files = FileUtil.listFiles(new File(LOT1_SRC_ROOT), PATTERN);
		for (File file : files) {
			log.info("Processing: " + file.getName());
			CreateColumnMappingsAction.exec(file, conns);
		}
		log.info("Done creating column aliases.");
	}

	private static void createGroupTables(CosmosConnections conns) {
		log.info("Finalizing (creating group table)...");
		RawDataFileUploadParams params = getParams();
		String projectCode = params.getProjCode();
		List<RawTableGroupDvo> list = Dao.findList(new RawTableGroupDvo(), "project", projectCode, conns.getMySqlConnection());
		for (RawTableGroupDvo dvo : list) {
			String code = dvo.getCode();
			log.info("Creating group table for: " + code);
			CreateGrpDataTableAction.execute(code, conns, true);
		}
		log.info("Done with finalize");
	}

	public static void buildSchema(CosmosConnections conns) {
		log.info("Dropping database");
		DatabricksDbUtil.dropDatabase(PROJECT_CODE, conns.getDbConnection(), conns);
		log.info("Creating " + PROJECT_CODE + " schema");
		List<File> files = FileUtil.listFiles(new File(LOT1_SRC_ROOT), SQL_FILE_PATTERN);
		for (File file : files) {
			Database.executeSqlScript(file, conns.getDbConnection());
		}
		String metaSchemaName = PROJECT_CODE + "_meta";
		log.info("Dropping meta schema: " + metaSchemaName);
		DatabricksDbUtil.dropDatabase(metaSchemaName, conns.getDbConnection(), conns);
		log.info("Creating schema");
		DatabricksDbUtil.createDatabase(metaSchemaName, conns.getDbConnection());
		log.info("Creating meta data tables");
		CreateMetricsTable.createMetaSchema(PROJECT_CODE, metaSchemaName, conns);
		log.info("Done creating meta.");
	}

}
