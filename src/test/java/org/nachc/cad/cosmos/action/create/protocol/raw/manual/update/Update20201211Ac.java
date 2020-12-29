package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateObsGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20201211Ac {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2020-12-11-AC\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health/";

	public static void main(String[] args) {
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Adding update files...");
		updateFiles("Demographics", "demo", mySqlConn, dbConn);
		updateFiles("Diagnosis", "dx", mySqlConn, dbConn);
		updateFiles("Encounter", "enc", mySqlConn, dbConn);
		updateFiles("Observation", "obs", mySqlConn, dbConn);
		updateFiles("Procedure", "proc", mySqlConn, dbConn);
		updateFiles("Rx", "rx", mySqlConn, dbConn);
		log("Doing updates");
		new UpdateDemoGroupTable().doUpdate();
		new UpdateEncGroupTable().doUpdate();
		new UpdateObsGroupTable().doUpdate();
		new UpdateProcedureGroupTable().doUpdate();
		new UpdateRxGroupTable().doUpdate();
		log.info("Done.");
	}

	private static void updateFiles(String name, String abr, Connection mySqlConn, Connection dbConn) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, mySqlConn);
		if ("obs".equals(abr)) {
			params.setDelimiter('|');
		}
		log.info("Uploading " + name + " using: " + params.getDelimiter());
		UploadRawDataFiles.updateExistingEntity(params, true);
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), dbConn, mySqlConn, true);
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

	public static RawDataFileUploadParams getParams(String name, String abr, Connection mySqlConn) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		params.setProjCode("womens_health");
		params.setDataLot("LOT 2");
		params.setDatabricksFileLocation(DATABRICKS_FILE_ROOT + abr);
		params.setDataGroupName(name);
		params.setDataGroupAbr(abr);
		String localHostFileAbsLocation = SRC_ROOT + abr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		String code = params.getRawTableGroupCode();
		log.info("Getting raw_table_group for: " + code);
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, mySqlConn);
		params.setRawTableGroupDvo(rawTableGroupDvo);
		return params;
	}

}
