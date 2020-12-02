package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20201127Proc {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2020-11-27-proc\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/integration-test/womens-health/";

	public static void main(String[] args) {
		log.info("Adding update files...");
		updateFiles("ProcCat", "proc_cat");
		log("Doing updates");
		log.info("Done.");
	}

	private static void updateFiles(String name, String abr) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr);
		UploadRawDataFiles.updateExistingEntity(params, true);
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		CreateGrpDataTableAction.execute(params, dbConn, mySqlConn, true);
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

	public static RawDataFileUploadParams getParams(String name, String abr) {
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
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		// create the raw table group (if it doesn't exist)
		createRawTableGroupIfItDoesNotExist(params, conn);
		// create the databricks schemas if they do not exist
		createDatabricksSchemasIfTheyDoNotExist(params);
		// get the group
		String code = params.getRawTableGroupCode();
		log.info("Getting raw_table_group for: " + code);
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, conn);
		params.setRawTableGroupDvo(rawTableGroupDvo);
		// done
		return params;
	}

	private static void createRawTableGroupIfItDoesNotExist(RawDataFileUploadParams params, Connection conn) {
		log.info("Creating raw_table_group record");
		log.info("Looking for: " + params.getRawTableGroupCode());
		RawTableGroupDvo dvo = Dao.find(new RawTableGroupDvo(), "code", params.getRawTableGroupCode(), conn);
		if (dvo == null) {
			dvo = CreateRawTableGroupRecordAction.execute(params, conn);
		}
		Database.commit(conn);
		log.info("created raw_table_group record: " + dvo.getGuid());
		log.info("Done with create raw table group");
	}

	private static void createDatabricksSchemasIfTheyDoNotExist(RawDataFileUploadParams params) {
		log.info("Getting connection...");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		String schemaName;
		// raw data schema
		schemaName = params.getRawTableSchemaName();
		if(DatabricksDbUtil.databaseExists(schemaName, conn) == false) {
			log.info("* * * CREATING SCHEMA: " + schemaName + " * * *");
			DatabricksDbUtil.createDatabase(schemaName, conn);
		}
		// group data schema
		schemaName = params.getGroupTableSchemaName();
		if(DatabricksDbUtil.databaseExists(schemaName, conn) == false) {
			log.info("* * * CREATING SCHEMA: " + schemaName + " * * *");
			DatabricksDbUtil.createDatabase(schemaName, conn);
		}
	}
	
}
