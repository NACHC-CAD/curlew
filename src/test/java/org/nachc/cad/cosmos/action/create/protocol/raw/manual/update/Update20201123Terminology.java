package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20201123Terminology {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2020-11-23-term\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health/";

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Adding update files...");
			updateFiles("MedDescriptionCat", "med_description_cat", conns);
			conns.commit();
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

	private static void updateFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, conns);
		UploadRawDataFiles.updateExistingEntity(params, conns, true);
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
	}

	private static void log(String msg) {
		String logMsg = "\n\n\n\n\n";
		logMsg += "* ----------------------------------------------------------------- \n";
		logMsg += "*\n";
		logMsg += "* " + msg + "\n";
		logMsg += "*\n";
		logMsg += "* ----------------------------------------------------------------- \n";
		log.info(logMsg);
	}

	public static RawDataFileUploadParams getParams(String name, String abr, CosmosConnections conns) {
		// get the parameters and connection
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		params.setProjCode("womens_health");
		params.setDataLot("LOT 1");
		params.setDatabricksFileLocation(DATABRICKS_FILE_ROOT + abr);
		params.setDataGroupName(name);
		params.setDataGroupAbr(abr);
		String localHostFileAbsLocation = SRC_ROOT + abr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		// create the raw table group (if it doesn't exist)
		createRawTableGroupIfItDoesNotExist(params, conns);
		// create the databricks schemas if they do not exist
		createDatabricksSchemasIfTheyDoNotExist(params, conns);
		// get the group
		String code = params.getRawTableGroupCode();
		log.info("Getting raw_table_group for: " + code);
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, conns.getMySqlConnection());
		params.setRawTableGroupDvo(rawTableGroupDvo);
		// done
		return params;
	}

	private static void createRawTableGroupIfItDoesNotExist(RawDataFileUploadParams params, CosmosConnections conns) {
		log.info("Creating raw_table_group record");
		log.info("Looking for: " + params.getRawTableGroupCode());
		RawTableGroupDvo dvo = Dao.find(new RawTableGroupDvo(), "code", params.getRawTableGroupCode(), conns.getMySqlConnection());
		if (dvo == null) {
			dvo = CreateRawTableGroupRecordAction.execute(params, conns.getMySqlConnection());
		}
		Database.commit(conns.getMySqlConnection());
		log.info("created raw_table_group record: " + dvo.getGuid());
		log.info("Done with create raw table group");
	}

	private static void createDatabricksSchemasIfTheyDoNotExist(RawDataFileUploadParams params, CosmosConnections conns) {
		Connection conn = conns.getDbConnection();
		String schemaName;
		// raw data schema
		schemaName = params.getRawTableSchemaName();
		if(DatabricksDbUtil.databaseExists(schemaName, conn, conns) == false) {
			log.info("* * * CREATING SCHEMA: " + schemaName + " * * *");
			DatabricksDbUtil.createDatabase(schemaName, conn);
		}
		// group data schema
		schemaName = params.getGroupTableSchemaName();
		if(DatabricksDbUtil.databaseExists(schemaName, conn, conns) == false) {
			log.info("* * * CREATING SCHEMA: " + schemaName + " * * *");
			DatabricksDbUtil.createDatabase(schemaName, conn);
		}
	}
	
}
