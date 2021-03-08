package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateOtherGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20201121 {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2020-11-21\\csv\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health/";

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Adding update files...");
			updateFiles("Demographics", "demo", conns);
			updateFiles("Encounter", "enc", conns);
			updateFiles("Other", "other", conns);
			updateFiles("Procedure", "proc", conns);
			updateFiles("Rx", "rx", conns);
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

	private static void updateFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, conns.getMySqlConnection());
		UploadRawDataFiles.updateExistingEntity(params, conns, true);
		// CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
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
